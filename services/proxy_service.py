import random
import os

def get_random_proxy_ip(proxy_file_path):
    """
    Reads a file containing proxy IPs, removes blank lines, and returns a random proxy IP.

    :param proxy_file_path: Path to the file containing proxy IPs.
    :return: A randomly selected proxy IP.
    :raises IOError: If the file is empty or does not contain valid data.
    """
    if not os.path.exists(proxy_file_path):
        raise IOError(f"The file '{proxy_file_path}' does not exist.")

    with open(proxy_file_path, 'r', encoding='utf-8') as file:
        proxies = [line.strip() for line in file if line.strip()]

    if not proxies:
        raise IOError("The proxy file is empty or contains no valid data.")

    return random.choice(proxies)
    