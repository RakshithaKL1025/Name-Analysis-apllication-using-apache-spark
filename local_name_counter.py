def count_names_with_letter(names_list, target_letter):
    """
    Count names containing a specific letter (non-Spark version)
    
    Args:
        names_list (list): List of names to analyze
        target_letter (str): Letter to search for in names
    
    Returns:
        int: Count of names containing the target letter
    """
    count = 0
    matching_names = []
    
    # Convert target letter to lowercase for case-insensitive comparison
    target_lower = target_letter.lower()
    
    # Iterate through names and count matches
    for name in names_list:
        if target_lower in name.lower():
            count += 1
            matching_names.append(name)
    
    # Print the names containing the letter
    print(f"Names containing '{target_letter}':")
    for name in matching_names:
        print(f"  {name}")
    
    return count

def count_letter_occurrences(names_list, target_letter):
    """
    Count total occurrences of a letter across all names
    
    Args:
        names_list (list): List of names to analyze
        target_letter (str): Letter to count occurrences of
    
    Returns:
        int: Total occurrences of the target letter
    """
    total_count = 0
    target_lower = target_letter.lower()
    
    # Count occurrences in each name
    for name in names_list:
        occurrences = name.lower().count(target_lower)
        total_count += occurrences
        if occurrences > 0:
            print(f"  '{name}' contains {occurrences} occurrence(s) of '{target_letter}'")
    
    return total_count

def save_results_to_file(names_list, target_letter, filename="name_count_results.txt"):
    """
    Save the name counting results to a file
    
    Args:
        names_list (list): List of names to analyze
        target_letter (str): Letter to search for in names
        filename (str): Name of file to save results to
    """
    # Get results
    matching_names = [name for name in names_list if target_letter.lower() in name.lower()]
    name_count = len(matching_names)
    total_occurrences = sum(name.lower().count(target_letter.lower()) for name in names_list)
    
    # Write results to file
    with open(filename, 'w') as f:
        f.write("=" * 50 + "\n")
        f.write("SPARK BIG DATA NAME COUNTER RESULTS\n")
        f.write("=" * 50 + "\n\n")
        
        f.write(f"Analysis Date: {__import__('datetime').datetime.now()}\n")
        f.write(f"Target Letter: '{target_letter}'\n")
        f.write(f"Total Names Processed: {len(names_list)}\n\n")
        
        f.write(f"Names containing '{target_letter}':\n")
        for name in matching_names:
            f.write(f"  {name}\n")
        f.write("\n")
        
        f.write(f"Number of names containing '{target_letter}': {name_count}\n")
        f.write(f"Total occurrences of '{target_letter}' across all names: {total_occurrences}\n\n")
        
        f.write("Full name list:\n")
        for name in names_list:
            occurrences = name.lower().count(target_letter.lower())
            f.write(f"  {name} ({occurrences} occurrence(s) of '{target_letter}')\n")
    
    print(f"Results saved to {filename}")
    return filename

# Example usage
if __name__ == "__main__":
    # Sample data
    sample_names = [
        "Alice", "Bob", "Charlie", "David", "Eve", 
        "Frank", "Grace", "Henry", "Ivy", "Jack",
        "Kate", "Liam", "Mia", "Noah", "Olivia"
    ]
    
    target = "a"
    
    print("=== Basic Count ===")
    count = count_names_with_letter(sample_names, target)
    print(f"Number of names containing '{target}': {count}")
    
    print("\n=== Letter Occurrence Count ===")
    total_occurrences = count_letter_occurrences(sample_names, target)
    print(f"Total occurrences of '{target}' across all names: {total_occurrences}")
    
    print("\n=== Saving Results to File ===")
    filename = save_results_to_file(sample_names, target)
    print(f"Output has been saved to: {filename}")