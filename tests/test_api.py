from src.api import split_quote, Quote


def test_split_quote_lead_in():
    input = """On chess...
'[to succeed] you must study the endgame before everything else.'
Jose Raul Capablanca"""
    res = split_quote(input)
    expected_res = Quote(
        lead_in="On chess...",
        content="'[to succeed] you must study the endgame before everything else.'",
        source="Jose Raul Capablanca",
    )
    assert res == expected_res


def test_split_quote_source():
    input = """'Sometimes even good Homer nods off.'
Horace, Ars Poetica"""
    res = split_quote(input)
    expected_res = Quote(
        lead_in="",
        content="'Sometimes even good Homer nods off.'",
        source="Horace, Ars Poetica",
    )
    assert res == expected_res
