from typing import Dict
import difflib

def revert_dict(dictionary: Dict) -> Dict:
    """
    Reverts a dictionary by swapping its keys and values.
    Input dict has like structure:

    >>> {
    >>>     "key": [values]
    >>> }

    Args:
    dict (dict): The input dictionary to be reverted.

    Returns:
    dict: A new dictionary with keys and values swapped.

    Raises:
    TypeError: If any value in the input dictionary is not hashable.
    ValueError: If 

    Notes:
    - If the input dictionary is empty, an empty dictionary is returned.
    - If there are duplicate values in the input dictionary, the reverted dictionary
      will contain only the last key-value pair for that value.
    """
    if not dictionary:
        raise ValueError("Input dictionary is empty.")
    if not isinstance(dictionary, dict):
        raise TypeError("Input must be a dictionary.")
    try:
        mapping = {
            raw_name: std_name
            for std_name, raw_names in dictionary.items()
            for raw_name in raw_names
        }
        return mapping
    except TypeError as e:
        raise TypeError("All values in the input dictionary must be hashable.") from e
    

class Mapping():
    def __init__(self, mapping: Dict, threshold: float = 0.8) -> None:
        if not mapping:
            raise ValueError("Mapping is empty.")
        if not isinstance(mapping, dict):
            raise TypeError("Mapping must be a dictionary.")
        if any(isinstance(v, (list, tuple)) for v in mapping.values()) :
            self.mapping = revert_dict(mapping)
        else:
            self.mapping = mapping
        self.threshold = threshold
    
    def get(self, key: str) -> str:
        """
        Retrieves the value associated with the given key from the mapping.
        If the key is not found, returns the key itself.

        Args:
            key (str): The key to look for in the mapping.

        Returns:
            str: The value associated with the key if found, otherwise the key itself.
        """
        if key in self.mapping:
            return self.mapping[key]
        matches = difflib.get_close_matches(key, self.mapping.keys(), n=1, cutoff=self.threshold)
        if matches:
            return self.mapping[matches[0]]
        else:
            print(self.mapping)
            raise KeyError(f"Key '{key}' not found in mapping.")