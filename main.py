from queue import Queue
from threading import Lock, Thread, current_thread
from typing import List, Optional, Set
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup


class Crawler(Thread):
    def __init__(self, start_url: str, queue: Queue, visited: Set[str], lock: Lock):
        Thread.__init__(self)
        print(f"crawler worker {current_thread().ident} is starting...")
        self.start_url = start_url
        self.queue = queue
        self.visited = visited
        self.lock = lock
        self.start_url_hostname = self.get_hostname(start_url)

    def get_hostname(self, url: str) -> Optional[str]:
        return urlparse(url).hostname

    def get_html(self, url: str) -> str:
        res = requests.get(url)
        return res.text

    def get_urls(self, url: str) -> List[str]:
        html = self.get_html(url)
        soup = BeautifulSoup(html, "html.parser")

        urls: List[str] = []
        for a_tag in soup.find_all("a"):
            href = a_tag.get("href")
            urls.append(href)

        return urls

    def get_absolute_url(self, url: str) -> str:
        return urljoin(self.start_url, url)

    def run(self):
        while not self.queue.empty():
            cur_url = self.queue.get()
            print(cur_url)

            crawled_urls = self.get_urls(cur_url)
            for crawled_url in crawled_urls:
                # convert relative url to absolute path
                absolute_url = self.get_absolute_url(crawled_url)

                # acquire visited lock
                self.lock.acquire()
                if (
                    absolute_url not in self.visited
                    and self.get_hostname(absolute_url) == self.start_url_hostname
                ):
                    self.visited.add(absolute_url)
                    self.queue.put(absolute_url)
                # release visited lock
                self.lock.release()

            self.queue.task_done()


if __name__ == "__main__":
    # num of thread
    num_thread = 16

    # start url
    start_url = "http://www.farbod.dev"

    # queue
    queue = Queue()
    queue.put(start_url)

    # visited set
    visited = set()

    # visited lock
    lock = Lock()

    # threads
    threads = []
    for _ in range(num_thread):
        crawler = Crawler(start_url, queue, visited, lock)
        crawler.start()
        threads.append(crawler)

    for thread in threads:
        thread.join()

    # blocks until all items in the Queue have been gotten and processed
    queue.join()

    # print all the visited urls
    print(list(visited))
