"""
Detailed test demonstrating the realized gain calculation bug.

The bug: Realized gains calculated as (withdrawal + capital - deposit) 
fails when sale proceeds are reinvested because capital decreases.
"""

def demonstrate_bug():
    print("=== Realized Gain Calculation Bug Demonstration ===\n")
    
    print("Scenario:")
    print("1. Deposit 100 SEK")
    print("2. Buy Asset A for 100 SEK")
    print("3. Asset A appreciates to 150 SEK")
    print("4. Sell Asset A for 150 SEK (50 SEK gain)")
    print("5. Buy Asset B for 150 SEK (reinvest)\n")
    
    print("Current formula: realized_gainloss = withdrawal + capital - deposit")
    print("\nStep-by-step calculation:")
    
    # Step 1: Deposit
    deposit = 100
    capital = 100  # Cash from deposit
    withdrawal = 0
    realized = withdrawal + capital - deposit
    print(f"1. After deposit: deposit={deposit}, capital={capital}, withdrawal={withdrawal}")
    print(f"   realized = {withdrawal} + {capital} - {deposit} = {realized}")
    
    # Step 2: Buy Asset A  
    capital = 0  # Cash used for purchase
    realized = withdrawal + capital - deposit
    print(f"\n2. After buying Asset A: deposit={deposit}, capital={capital}, withdrawal={withdrawal}")
    print(f"   realized = {withdrawal} + {capital} - {deposit} = {realized}")
    
    # Step 3: Asset appreciates (no transaction)
    print(f"\n3. Asset A appreciates to 150 SEK (no transaction)")
    print(f"   Portfolio value = Asset A (150) + cash (0) = 150")
    
    # Step 4: Sell Asset A
    capital = 150  # Cash from sale
    realized = withdrawal + capital - deposit
    print(f"\n4. After selling Asset A: deposit={deposit}, capital={capital}, withdrawal={withdrawal}")
    print(f"   realized = {withdrawal} + {capital} - {deposit} = {realized} ✓ (CORRECT!)")
    
    # Step 5: Buy Asset B (reinvest)
    capital = 0  # Cash used for purchase
    realized = withdrawal + capital - deposit
    print(f"\n5. After buying Asset B (reinvest): deposit={deposit}, capital={capital}, withdrawal={withdrawal}")
    print(f"   realized = {withdrawal} + {capital} - {deposit} = {realized} ❌ (BUG!)")
    
    print("\n=== BUG SUMMARY ===")
    print("The 50 SEK gain from selling Asset A disappears when proceeds are reinvested.")
    print("The formula treats reinvestment as if the gain never happened.")
    print("\nCorrect approach should track:")
    print("  Realized gain = sale_price - purchase_price")
    print("  = 150 - 100 = 50 SEK")
    print("This should persist regardless of reinvestment.")

if __name__ == "__main__":
    demonstrate_bug()
