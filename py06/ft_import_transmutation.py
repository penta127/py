
def main() -> None:
    print()
    print("=== Import Transmutation Mastery ===")
    print()

    import alchemy.elements 
    print("Method 1 - Full module import:")
    print(f"alchemy.elements.create_fire(): {alchemy.elements.create_fire()}")
    print()

    from alchemy.elements import create_water
    print("Method 2 - Specific function import:")
    print(f"create_water(): {create_water()}")
    print()

    from alchemy.potions import healing_potion as heal
    print("Method 3 - Aliased import:")
    print(f"heal(): {heal()}")
    print()

    from alchemy import elements, potions
    print("Method 4 - Multiple imports:")
    print(f"create_earth(): {elements.create_earth()}")
    print(f"create_fire(): {elements.create_fire()}")
    print(f"strength_potion(): {potions.strength_potion()}")
    print()

    print("All import transmutation methods mastered!")


if __name__ == "__main__":
    main()