"""
Descobre quais tabs de mercado estao disponiveis na match page do oddsagora.com.br.

Seletores (2026-04):
  [data-testid="navigation-active-tab"]   - tab ativa atualmente
  [data-testid="navigation-inactive-tab"] - tabs inativas

Retorna lista de labels (strings em portugues/ingles como exibidas na UI).
Ex: ["Home/Away", "1X2", "Acima/Abaixo", "Handicap Asiatico", ...]
"""
from bs4 import BeautifulSoup


def parse(html: str) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    labels = []

    active = soup.select_one('[data-testid="navigation-active-tab"]')
    if active:
        t = active.get_text(strip=True)
        if t:
            labels.append(t)

    for el in soup.select('[data-testid="navigation-inactive-tab"]'):
        t = el.get_text(strip=True)
        if t and t not in labels:
            labels.append(t)

    return labels
