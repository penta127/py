#!/usr/bin/env python3

def ft_ancient_text() -> None:
    result = True
    filename = "ancient_fragment.txt"
    print("=== CYBER ARCHIVES - DATA RECOVERY SYSTEM ===")
    print()
    print(f"Accessing Storage Vault: {filename}")

    try:
        file = open(filename, "r")
        print("Connection established...")
        print()
        print("RECOVERED DATA:")
        try:
            data = file.read()
        except Exception:
            file.close()
            result == False
        if (result == True):
            file.close()
            print(data)
        print()
        print("Data recovery complete. Storage unit disconnected.")

    except Exception:
        print("ERROR: Storage vault not found. Run data generator first.")
        file.close()
        return

if __name__ == "__main__":
    ft_ancient_text()
