## Current work on HPT

#### Machine-learning pipeline

1. Plan is to use LabelStudio to test annotations for data cleanup

#### Labelstudio

Following this README.

https://labelstud.io/blog/evaluating-named-entity-recognition-parsers-with-spacy-and-label-studio/

Turns out you need to download this first:

`python -m spacy download en_core_web_lg`

Before tagging you need to do:

`label-studio init ner-tagging`

~ python3 example_cli.py --url "https://anthembcbsin.mrf.bcbs.com/2022-12_690_08B0_in-network-rates_15_of_35.json.gz?&Expires=1674069059&Signature=Mzf3xI1E2ZzUfSovjiAF9BmHJ7FyR7WMEw2EI3DygqiEwWfaSmczOrbFd9OKudZUkMED7tiFSDS9qMhMoPES8C7wxoMvZCzW2FPN8PuPTwk1U5vaY9ev7m8g28pjjmvP3HwLwSixQ2U5zvOs8yvFTIHF1YaIiFvnbIixyL3EfRTIX8aP6Rz7h2D6kM9U8r8Fc9-l3XV6fosgegTCbrtyKAoEEfsJLWv73wwDCddVyqXK6A5onsKz6vnDzjc9x8l4TdjLYakGP3EzSdgKf20VkX3l9CGngPYnH3PvPa-CBDbVYW02PYH3Q3k-udFZlB43PEv46QYDcG9P7g5jHTqR7w__&Key-Pair-Id=K27TQMT39R1C8A" --out out_dir/ --codes quest/week0/codes_prelim.csv --npis quest/week0/npis.csv
