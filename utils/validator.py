def validate_response(response, required_keys):
    """Validate that the response contains the required keys."""
    if not response:
        return False
    for key in required_keys:
        if key not in response:
            return False
    return True
