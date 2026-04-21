"""Tour screens must render in both EN and FR, all 5 steps.

The tour content lives in _TOUR_CONTENT. These tests pin:
- each of the 5 steps has both FR + EN strings,
- the renderer actually outputs the requested language's strings,
- the language switcher link flips to the other locale,
- the Finish button on step 5 POSTs to /tour/complete,
- step clamping (step=0 → 1, step=99 → 5) is idempotent.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations import gap_routes as gr  # noqa: E402


def test_all_5_screens_bilingual():
    assert gr.TOUR_TOTAL_STEPS == 5
    for i, screen in enumerate(gr._TOUR_CONTENT, 1):
        assert 'en' in screen, f"step {i} missing EN"
        assert 'fr' in screen, f"step {i} missing FR"
        for lang in ('en', 'fr'):
            block = screen[lang]
            assert block.get('title'), f"step {i}/{lang} missing title"
            assert block.get('body'), f"step {i}/{lang} missing body"
            assert block.get('bullets'), f"step {i}/{lang} missing bullets"


def test_tour_renders_in_english():
    html = gr.render_tour_screens(1, lang='en')
    assert 'Welcome to OtoCPA' in html
    assert 'Step 1 of 5' in html
    # FR strings must NOT leak into an EN render
    assert 'Bienvenue' not in html


def test_tour_renders_in_french():
    html = gr.render_tour_screens(1, lang='fr')
    assert 'Bienvenue sur OtoCPA' in html
    assert 'Étape 1 sur 5' in html
    # EN strings must NOT leak into an FR render
    assert 'Welcome to OtoCPA' not in html


def test_language_switcher_flips_locale():
    html_en = gr.render_tour_screens(1, lang='en')
    assert '/tour?step=1&amp;lang=fr' in html_en or '/tour?step=1&lang=fr' in html_en
    html_fr = gr.render_tour_screens(1, lang='fr')
    assert '/tour?step=1&amp;lang=en' in html_fr or '/tour?step=1&lang=en' in html_fr


def test_every_step_bilingual_rendering():
    for step in range(1, 6):
        en = gr.render_tour_screens(step, lang='en')
        fr = gr.render_tour_screens(step, lang='fr')
        assert f'data-tour-step="{step}"' in en
        assert f'data-tour-step="{step}"' in fr
        assert 'Step' in en  # "Step N of 5"
        assert 'Étape' in fr  # "Étape N sur 5"


def test_step_5_has_finish_button_not_next():
    html = gr.render_tour_screens(5, lang='en')
    assert 'Finish tour' in html
    assert '/tour/complete' in html
    # Next arrow should NOT appear on the last step.
    assert 'Next &rarr;' not in html


def test_step_1_has_no_back_button():
    html = gr.render_tour_screens(1, lang='en')
    # The Back link only appears from step 2 onwards.
    assert '&larr; Back' not in html


def test_step_clamping():
    # step=0 → clamps to 1
    html_low = gr.render_tour_screens(0, lang='en')
    assert 'Step 1 of 5' in html_low
    # step=99 → clamps to 5
    html_high = gr.render_tour_screens(99, lang='en')
    assert 'Step 5 of 5' in html_high


def test_try_it_link_present():
    # Every screen has a try_label + try_href
    for step in range(1, 6):
        html = gr.render_tour_screens(step, lang='en')
        assert 'Try it:' in html or 'Essayez' in html


def test_unknown_lang_falls_back_to_english():
    html = gr.render_tour_screens(1, lang='de')
    assert 'Welcome to OtoCPA' in html
