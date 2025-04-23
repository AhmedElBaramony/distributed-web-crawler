
from mpi4py import MPI
import time
import logging
import requests
from bs4 import BeautifulSoup

# Configure logging format
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Crawler - %(levelname)s - %(message)s')

def crawler_process():
    """
    Crawler Node: Fetches webpages and extracts data.
    Responsibilities:
    - Receive crawl tasks from master (tag 0)
    - Fetch and parse web pages
    - Send discovered URLs back to master (tag 1)
    - Send content to indexer (tag 2)
    - Send status (tag 99) or errors (tag 999)
    """
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    logging.info(f"Crawler node {rank} started out of {size} total nodes.")

    while True:
        status = MPI.Status()
        url = comm.recv(source=0, tag=0, status=status)  # Tag 0: new task
        if not url:
            logging.info("Received shutdown signal.")
            break

        logging.info(f"Received URL: {url}")
        try:
            # Fetch web page
            response = requests.get(url, timeout=5)
            soup = BeautifulSoup(response.text, 'html.parser')
            text = soup.get_text()

            # Extract new links (simulate for now)
            new_links = [a['href'] for a in soup.find_all('a', href=True)][:2]

            # Send results
            comm.send(new_links, dest=0, tag=1)  # Tag 1: discovered URLs
            comm.send(text[:500], dest=size - 1, tag=2)  # Tag 2: content to indexer
            comm.send(f"Successfully crawled {url}", dest=0, tag=99)  # Tag 99: status
        except Exception as e:
            logging.error(f"Error crawling {url}: {e}")
            comm.send(f"Error crawling {url}: {e}", dest=0, tag=999)  # Tag 999: error

if __name__ == '__main__':
    crawler_process()
