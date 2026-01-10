
def ft_ancient_text():
    filename = "ancient_fragment.txt"
    
    print("=== CYBER ARCHIVES - DATA RECOVERY SYSTEM ===")
    print()
    print(f"Accessing Storage Vault: {filename}")
    try:
        file = open(filename, "r")
    except FileNotFoundError:
        print("ERROR: Storage vault not found. Run data generator first.")
        return
    print("Connection established...")
    print()
    print("RECOVERED DATA:")
    data = file.read()
    file.close()
    print(data)
    print()
    print("Data recovery complete. Storage unit disconnected.")


if __name__ == "__main__":
    ft_ancient_text()
    