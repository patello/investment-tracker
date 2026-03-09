import pytest
from datetime import date
from database_handler import DatabaseHandler
from data_parser import DataParser
from calculate_stats import StatCalculator

@pytest.fixture
def dietz_scenario_db(tmp_path):
    """
    Creates a database with a specific scenario for testing the Modified Dietz Method:
    - 2017-01-01: Deposit 1000 SEK
    - 2017-01-02: Purchase Asset A for 1000 SEK (locks capital into an asset)
    - 2018-01-01: Sell half (500 SEK) and withdraw 500 SEK
    - 2019-01-01: Sell remainder (600 SEK - a 100 SEK gain) and withdraw 600 SEK
    """
    csv_content = """Datum;Konto;Typ av transaktion;Värdepapper/beskrivning;Antal;Kurs;Belopp;Courtage;Valuta;ISIN;Resultat
2017-01-01;1111;Insättning;Deposit;-;-;1000;0;SEK;;-
2017-01-02;1111;Köp;Asset A;100;10;-1000;0;SEK;TESTA;-
2018-01-01;1111;Sälj;Asset A;-50;10;500;0;SEK;TESTA;-
2018-01-02;1111;Uttag;;-;-;-500;0;SEK;;-
2019-01-01;1111;Sälj;Asset A;-50;12;600;0;SEK;TESTA;-
2019-01-02;1111;Uttag;;-;-;-600;0;SEK;;-
"""
    csv_file = tmp_path / "dietz_scenario.csv"
    csv_file.write_text(csv_content, encoding="utf-8")
    
    db_file = tmp_path / "test_modified_dietz.db"
    db = DatabaseHandler(db_file)
    
    parser = DataParser(db)
    parser.add_data(str(csv_file))
    parser.process_transactions()
    
    return db

def test_modified_dietz_weights_and_time_horizon(dietz_scenario_db):
    """
    Tests that the Modified Dietz APY calculation applies multiple weights
    for the exact withdrawal dates and stops the time horizon when the 
    position is fully closed in 2019, rather than continuing to the current date.
    """
    stat_calculator = StatCalculator(dietz_scenario_db)
    stat_calculator.calculate_stats()
    
    # Fetch stats including closed positions (deposits="all")
    stats = stat_calculator.get_stats(accounts=["1111"], period="year", deposits="all")
    
    # Look for the cohort originating from 2017
    cohort_2017 = next((s for s in stats if s[0].year == 2017), None)
    assert cohort_2017 is not None, "2017 cohort should exist"
    
    # Indices based on StatCalculator.get_stats return tuple:
    # 1: deposit, 2: withdrawal, 3: value, 4: total_gainloss, 10: annual_per_yield
    deposit = cohort_2017[1]
    withdrawal = cohort_2017[2]
    value = cohort_2017[3]
    total_gainloss = cohort_2017[4]
    apy = cohort_2017[10]
    
    assert deposit == 1000.0
    assert withdrawal == 1100.0  # 500 + 600
    assert value == 0.0          # Position fully closed
    assert total_gainloss == 100.0
    
    # Mathematical expectation for Modified Dietz in this scenario:
    # Start date: 2017-01-01 (V0 = 1000)
    # End date (fully closed): 2019-01-02 (~731 days total)
    #
    # CF1: -500 on 2018-01-02 (~366 days in). Weight = (731 - 366) / 731 ≈ 0.4993
    # CF2: -600 on 2019-01-02 (731 days in). Weight = (731 - 731) / 731 = 0
    #
    # Sum(W * CF) = 0.4993 * (-500) = -249.65
    # Denominator = V0 + Sum(W * CF) = 1000 - 249.65 = 750.35
    # HPR = Gain / Denominator = 100 / 750.35 ≈ 13.32%
    # Annualized (over ~2 years) = (1 + 0.1332)^(365.25 / 731) - 1 ≈ 6.45%
    
    assert apy is not None, "APY should be calculated for historical positions"
    
    # Approximate assertion allowing minor float/day-count variation
    assert 6.40 <= apy <= 6.50, f"Expected APY around 6.45%, got {apy}%"