def ft_inventory_system():
    players = dict()
    players.update({"Alice": dict(), "Bob": dict()})

    players["Alice"].update({
        "sword": {"type": "weapon", "rarity": "rare", "qty": 1, "value": 500},
        "potion": {
            "type": "consumable",
            "rarity": "common",
            "qty": 5,
            "value": 50
        },
        "shield": {
            "type": "armor",
            "rarity": "uncommon",
            "qty": 1,
            "value": 200
        }
    })

    players["Bob"].update({
        "potion": {
            "type": "consumable",
            "rarity": "common",
            "qty": 0,
            "value": 50
        },
        "magic_ring": {
            "type": "accessory",
            "rarity": "rare",
            "qty": 1,
            "value": 300
        }
    })

    print("=== Player Inventory System ===")
    print()
    print("=== Alice's Inventory ===")

    total_value = 0
    item_total = 0
    categories = dict()

    for item_name, info in players.get("Alice", dict()).items():
        qty = info.get("qty", 0)
        value = info.get("value", 0)
        item_type = info.get("type", "unknown")
        rarity = info.get("rarity", "unknown")

        total = qty * value
        total_value = total_value + total
        item_total = item_total + qty
        categories[item_type] = categories.get(item_type, 0) + qty

        print(
            f"{item_name} ({item_type}, {rarity}): {qty}x @ "
            f"{value} gold each = {total} gold")
    print()
    print(f"Inventory value: {total_value} gold")
    print(f"Item count: {item_total} items")

    line = "Categories: "
    first = True
    for cat, q in categories.items():
        if first:
            line = line + f"{cat}({q})"
            first = False
        else:
            line = line + f", {cat}({q})"
    print(line)
    print()
    print("=== Transaction: Alice gives Bob 2 potions ===")

    alice_qty = players["Alice"]["potion"].get("qty", 0)
    players["Alice"]["potion"].update({"qty": alice_qty - 2})

    bob_qty = players["Bob"]["potion"].get("qty", 0)
    players["Bob"]["potion"].update({"qty": bob_qty + 2})

    print("Transaction successful!")
    print()
    print("=== Updated Inventories ===")
    print(f"Alice potions: {players['Alice']['potion'].get('qty', 0)}")
    print(f"Bob potions: {players['Bob']['potion'].get('qty', 0)}")
    print()
    print("=== Inventory Analytics ===")

    best_value_name = ""
    best_value = -1
    best_items_name = ""
    best_items = -1
    rare_names = dict()

    for player_name, inv in players.items():
        player_value = 0
        player_items = 0

        for item_name, info in inv.items():
            qty = info.get("qty", 0)
            value = info.get("value", 0)

            player_items = player_items + qty
            player_value = player_value + (qty * value)

            if info.get("rarity") == "rare":
                rare_names.update({item_name: True})

        if player_value > best_value:
            best_value = player_value
            best_value_name = player_name

        if player_items > best_items:
            best_items = player_items
            best_items_name = player_name

    print(f"Most valuable player: {best_value_name} ({best_value} gold)")
    print(f"Most items: {best_items_name} ({best_items} items)")

    rare_line = ""
    first = True
    for name in rare_names.keys():
        if first:
            rare_line = name
            first = False
        else:
            rare_line = rare_line + ", " + name
    print(f"Rarest items: {rare_line}")


if __name__ == "__main__":
    ft_inventory_system()
