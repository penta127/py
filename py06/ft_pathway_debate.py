
def main() -> None:
    print()
    print("=== Pathway Debate Mastery ===")
    print()

    from alchemy.transmutation.basic import lead_to_gold, stone_to_gem
    print("Testing Absolute Imports (from basic.py):")
    print(f"lead_to_gold(): {lead_to_gold()}")
    print(f"stone_to_gem(): {stone_to_gem()}")
    print()

    from alchemy.transmutation.advanced import philosophers_stone, elixir_of_life
    print("Testing Relative Imports (from advanced.py):")
    print(f"philosophers_stone(): {philosophers_stone()}")
    print(f"elixir_of_life(): {elixir_of_life()}")
    print()

    print("Testing Package Access:")
    print(f"alchemy.transmutation.lead_to_gold(): {lead_to_gold()}")
    print(f"alchemy.transmutation.philosophers_stone(): {philosophers_stone()}")
    print()

    print("Both pathways work! Absolute: clear, Relative: concise")

if __name__ == "__main__":
    main()