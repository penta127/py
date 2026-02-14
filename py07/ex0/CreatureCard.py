from .Card import Card

class CreatureCard(Card):

    def __init__(self, name: str, cost: int, rarity: str, attack: int, health: int) -> None:
        super().__init__(name, cost, rarity)
        self._attack = int(attack)
        if (self._attack <= 0):
            raise ValueError
        self._health = int(health)
        if (self._health <= 0):
            raise ValueError


    def play(self, game_state: dict) -> dict:
        play_dict = {
            "card_played" : self._name,
            "mana_used" : self._cost,
            "effect" : "Creature summoned to battlefield",
        }

        return play_dict

    def attack_target(self, target) -> dict:
        attack_dict = {
            "attacker" : self._name,
            "target" : target,
            "damage_dealt" : self._attack,
            "combat_resolved" : True,
        }

        return attack_dict
