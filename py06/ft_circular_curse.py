
from alchemy.grimoire import validate_ingredients, record_spell

def main() -> None:

    print()
    print("=== Circular Curse Breaking ===")
    print()

    print("Testing ingredient validation:")
    test_ingredient = ["fire air", "dragon scales"]
    for test in test_ingredient:
        print(f'validate_ingredients("{test}"): {validate_ingredients(test)}')
    print()

    print("Testing spell recording with validation:")
    test_ingredient = ["fire air", "shadow"]
    test_spell = ["Fireball", "Dark Magic"]
    for ingre, spell in zip(test_ingredient, test_spell):
        print(f'record_spell("{spell}", "{ingre}"): {record_spell(spell, ingre)}')
    print()

    print("Testing late import technique:")
    print(f'record_spell("Lightning", "air"): {record_spell("Lightning", "air")}')

    print()
    print("Circular dependency curse avoided using late imports!")
    print("All spells processed safely!")


if __name__ == "__main__":
    main()
