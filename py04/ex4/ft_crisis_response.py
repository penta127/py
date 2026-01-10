
def crisis_helper(filename, routine):
    if routine:
        print(f"ROUTINE ACCESS: Attempting access to '{filename}'...")
    else:
        print(f"CRISIS ALERT: Attempting access to '{filename}'...")
    try:
        with open(filename, "r") as file:
            data = file.read()
        
        print(f"SUCCESS: Archive recovered - ''{data}''")
        print("STATUS: Normal operations resumed")

    except FileNotFoundError:
        print("RESPONSE: Archive not found in storage matrix")
        print("STATUS: Crisis handled, system stable")

    except PermissionError:
        print("RESPONSE: Security protocols deny access")
        print("STATUS: Crisis handled, security maintained")

    except Exception as e:
        print(f"RESPONSE: {type(e).__name__}")
        print("STATUS: Crisis handled, system stable")




def ft_crisis_response():
    filename1 = "lost_archive.txt"
    filename2 = "classified_vault.txt"
    filename3 = "standard_archive.txt"
    print("=== CYBER ARCHIVES - CRISIS RESPONSE SYSTEM ===")
    print()
    crisis_helper(filename1, routine = False)
    print()
    crisis_helper(filename2, routine = False)
    print()
    crisis_helper(filename3, routine = True)

    print()
    print("All crisis scenarios handled successfully. Archives secure.")

if __name__ == "__main__":
    ft_crisis_response()