"""
Wrapper Playwright com stealth, rate-limit e retry.

Pool de paginas persistentes: N contextos criados uma vez em __aenter__ e
reutilizados via asyncio.Queue durante toda a sessao. Elimina o overhead de
criar/destruir BrowserContext a cada fetch() (~500ms + stealth setup).

Uso:
    async with OddsPortalBrowser(headful=False, concurrency=8) as browser:
        html = await browser.fetch("https://www.oddsportal.com/...")
"""
import asyncio
import os
import random
from contextlib import asynccontextmanager

from playwright.async_api import async_playwright
from tenacity import retry, stop_after_attempt, wait_exponential

try:
    from playwright_stealth import Stealth as _Stealth
    _stealth = _Stealth()
    async def stealth_async(page):
        await _stealth.apply_stealth_async(page)
    _HAS_STEALTH = True
except Exception:
    async def stealth_async(page):
        pass
    _HAS_STEALTH = False

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
]

NAV_TIMEOUT_MS  = 30_000
WAIT_TIMEOUT_MS = 15_000   # mantido alto de proposito: com o cache de seasons vazias,
                           # um timeout curto poderia gravar um false-empty (pagina
                           # valida lenta) como vazia PERMANENTE -> perda de cobertura.

# Recursos que NAO precisamos (so lemos o DOM via data-testid): bloquear acelera
# a carga (menos bytes/render) sem mudar os dados. Mantemos document/script/xhr/fetch.
_BLOCKED_RESOURCES = {"image", "media", "font"}
ODDS_WAIT_SELECTOR = '[data-testid="over-under-expanded-row"], [data-testid="navigation-active-tab"]'


class OddsPortalBrowser:
    def __init__(self, headful: bool = False, concurrency: int = 8):
        self.headful     = headful
        self.concurrency = concurrency
        self._pw         = None
        self._browser    = None
        self._pool: asyncio.Queue = None   # fila de paginas disponiveis

    async def _make_page(self):
        """Cria um contexto + pagina com stealth. Chamado apenas no __aenter__."""
        ua  = random.choice(USER_AGENTS)
        ctx = await self._browser.new_context(
            user_agent=ua,
            locale="en-US",
            timezone_id="UTC",
            viewport={"width": 1366, "height": 900},
        )
        page = await ctx.new_page()

        # Bloqueia imagens/midia/fontes (nao afetam o DOM que parseamos) — reduz
        # bytes e tempo de render. Script/xhr/fetch/document passam (trazem os dados).
        async def _block(route):
            try:
                if route.request.resource_type in _BLOCKED_RESOURCES:
                    await route.abort()
                else:
                    await route.continue_()
            except Exception:
                try:
                    await route.continue_()
                except Exception:
                    pass
        try:
            await page.route("**/*", _block)
        except Exception:
            pass

        if _HAS_STEALTH:
            try:
                await stealth_async(page)
            except Exception:
                pass
        return page

    async def __aenter__(self):
        self._pw = await async_playwright().start()
        proxy = os.environ.get("ODDSPORTAL_PROXY")
        launch_args = {
            "headless": not self.headful,
            # --disable-dev-shm-usage: /dev/shm no ubuntu-latest tem ~metade da RAM;
            # com 28+ renderers simultaneos pode saturar e causar crashes silenciosos.
            # Esta flag faz o Chromium usar /tmp em vez de /dev/shm sem custo de perf.
            "args": ["--disable-dev-shm-usage"],
        }
        if proxy:
            launch_args["proxy"] = {"server": proxy}
        self._browser = await self._pw.chromium.launch(**launch_args)

        # Cria pool de N paginas persistentes de uma so vez
        self._pool = asyncio.Queue()
        for _ in range(self.concurrency):
            page = await self._make_page()
            await self._pool.put(page)

        return self

    async def __aexit__(self, exc_type, exc, tb):
        # Fecha todos os contextos do pool
        while not self._pool.empty():
            try:
                page = self._pool.get_nowait()
                await page.context.close()
            except Exception:
                pass
        try:
            await self._browser.close()
        finally:
            await self._pw.stop()

    @asynccontextmanager
    async def _page(self):
        """Pega uma pagina do pool (bloqueia se todas ocupadas) e devolve ao terminar."""
        page = await self._pool.get()
        try:
            yield page
        except Exception:
            # Em caso de erro grave, recria a pagina antes de devolver ao pool
            try:
                await page.context.close()
            except Exception:
                pass
            try:
                page = await self._make_page()
            except Exception:
                pass
            raise
        finally:
            self._pool.put_nowait(page)

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=3, min=3, max=30),
        reraise=True,
    )
    async def fetch(self, url: str, wait_selector: str | None = ODDS_WAIT_SELECTOR) -> str:
        async with self._page() as page:
            await page.goto(url, timeout=NAV_TIMEOUT_MS, wait_until="domcontentloaded")
            if wait_selector:
                try:
                    await page.wait_for_selector(wait_selector, timeout=WAIT_TIMEOUT_MS)
                except Exception:
                    pass
            await asyncio.sleep(random.uniform(0.3, 0.7))
            return await page.content()

    async def fetch_all_tabs(self, match_url: str, tab_labels: list[str]) -> dict[str, str]:
        """
        Abre a match page e coleta HTML de cada tab de mercado clicando no elemento.

        tab_labels: lista de labels como exibidos na UI ("Home/Away", "1X2", etc.)
        Retorna {label: html}. O label "" nao e solicitado aqui; use self.fetch() para
        o HTML padrao da match page.
        """
        result = {}
        if not tab_labels:
            return result
        async with self._page() as page:
            await page.goto(match_url, timeout=NAV_TIMEOUT_MS, wait_until="domcontentloaded")
            try:
                await page.wait_for_selector(ODDS_WAIT_SELECTOR, timeout=WAIT_TIMEOUT_MS)
            except Exception:
                pass
            await asyncio.sleep(random.uniform(0.3, 0.7))

            # Captura tab ativa atual
            active_el = await page.query_selector('[data-testid="navigation-active-tab"]')
            active_label = (await active_el.inner_text()).strip() if active_el else ""
            if active_label:
                result[active_label] = await page.content()

            for label in tab_labels:
                if label == active_label:
                    continue
                inactive_tabs = await page.query_selector_all('[data-testid="navigation-inactive-tab"]')
                clicked = False
                for tab in inactive_tabs:
                    try:
                        text = (await tab.inner_text()).strip()
                        if text == label:
                            await tab.click()
                            try:
                                await page.wait_for_selector(
                                    '[data-testid="over-under-expanded-row"]',
                                    timeout=WAIT_TIMEOUT_MS,
                                )
                            except Exception:
                                pass
                            await asyncio.sleep(random.uniform(0.3, 0.6))
                            result[label] = await page.content()
                            clicked = True
                            break
                    except Exception:
                        continue
                # Tab nao encontrada / indisponivel — segue
        return result
