from .CreatureCard import CreatureCard
from tools.card_generator import CardGenerator


def main() -> None:
    print()
    print("=== DataDeck Card Foundation ===")
    print()
    print("Testing Abstract Base Class Design:")
    print()

    generator = CardGenerator()
    data = generator.get_creature("Fire Dragon")
    if data == None:
        return
    fire_dragon = CreatureCard(data["name"], data["cost"], data["rarity"], data["attack"], data["health"])
    print("CreatureCard Info:")
    print(f"{fire_dragon.get_card_info()}")
    print(f"Playable: {}")

    print("Playing Fire Dragon with 6 mana available:")



if __name__ == "__main__":
    main()
