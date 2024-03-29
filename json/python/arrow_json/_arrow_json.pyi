from typing import Any

def array_to_utf8_json_array(array: Any, large: bool = True) -> Any:
    """Encode a pyarrow  into a UTF-8 array of JSON data.

    Args:
        array (Any): The PyArrow array
        large (bool, optional): wether to use a LargeString array
            as the output or just a String array.
            If in doubt, use `large=True`.
            Defaults to True.

    Returns
        Any: A PyArrow array.
    """
    ...
