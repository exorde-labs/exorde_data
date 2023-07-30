import subprocess
import os
from time import sleep
import random
import re
from typing import AsyncGenerator
from urllib.parse import urlunparse, urlparse
from datetime import datetime

import logging
import asyncio
import hashlib
import pickle
from exorde_data import (
    Item,
    Content,
    Author,
    CreatedAt,
    Title,
    Url,
    Domain,
    ExternalId,
    ExternalParentId
)

from dotenv import dotenv_values
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.common import exceptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.window import WindowTypes

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
COOKIE_NAME = "linkedin_cookies.pkl"
DOMAIN = "www.linkedin.com"
# timeouts
PAGE_LOAD_WAIT_MIN_TIME = 1
PAGE_LOAD_WAIT_MAX_TIME = 2
FIND_ELEMENT_TIMEOUT = 2
# other search options
SORT_BY = "Latest"
DATE_POSTED = "Past month"
# default values
DEFAULT_OLDNESS_SECONDS = 120 
DEFAULT_MAXIMUM_ITEMS = 25
DEFAULT_MIN_POST_LENGTH = 10

global driver
driver = None


def wait_page_load():
    sleep(random.uniform(PAGE_LOAD_WAIT_MIN_TIME, PAGE_LOAD_WAIT_MAX_TIME))


def check_env() -> dict:
    # Checking the environment variables
    configs = dotenv_values(".env")
    username = configs["LINKEDIN_USERNAME"]
    password = configs["LINKEDIN_PASSWORD"]

    return {
        "username": username,
        "password": password
    }


def init_driver(headless=True, show_images=False, firefox=False):
    """ initiate a chromedriver or firefoxdriver instance
        --option : other option to add (str)
    """
    global driver
    options = ChromeOptions()
    # driver_path = chromedriver_autoinstaller.install()
    logging.info("Adding options to Chromium Driver")
    binary_path = get_chrome_path()
    options.binary_location = binary_path
    logging.info(f"\tSelected Chrome executable path = {binary_path}")
    options.add_argument("--no-sandbox")
    # Disable features that might betray automation
    options.add_argument("--disable-blink-features")
    # Disables a Chrome flag that shows an 'automation' toolbar
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option(
        "excludeSwitches", ["enable-automation"])  # Disable automation flags
    # Disable automation extensions
    options.add_experimental_option('useAutomationExtension', False)
    # Ensure GUI is off. Essential for Docker.
    options.add_argument("--disable-dev-shm-usage")
    # options.add_argument("user-data-dir=selenium")
    options.add_argument("disable-infobars")
    selected_user_agent = USER_AGENT
    options.add_argument(f'user-agent={selected_user_agent}')
    logging.info("\tselected_user_agent :  %s", selected_user_agent)

    if headless is True:
        options.add_argument("--headless")
        options.add_argument('--disable-gpu')
        options.headless = True
    else:
        options.headless = False
    options.add_argument('log-level=3')

    driver_path = '/opt/homebrew/bin/chromedriver'
    logging.info(f"Opening driver from path = {driver_path}")
    driver = webdriver.Chrome(options=options, executable_path=driver_path)

    driver.set_page_load_timeout(7)
    return driver

def check_and_kill_processes(process_names):
    for process_name in process_names:
        try:
            # Find processes by name
            result = subprocess.check_output(['pgrep', '-f', process_name])
            # If the previous command did not fail, we have some processes to kill
            if result:
                logging.info(
                    f"[Linkedin] Killing old chrome processes for: {process_name}")
                subprocess.run(["pkill", "-f", process_name])
        except subprocess.CalledProcessError:
            # If pgrep fails to find any processes, it throws an error. We catch that here and assume no processes are running
            logging.info(
                f"[Linkedin] No running chrome processes found for: {process_name}")


def get_chrome_path():
    return "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
    if os.path.isfile('/usr/bin/chromium-browser'):
        return '/usr/bin/chromium-browser'
    elif os.path.isfile('/usr/bin/chromium'):
        return '/usr/bin/chromium'
    elif os.path.isfile('/usr/bin/chrome'):
        return '/usr/bin/chrome'
    elif os.path.isfile('/usr/bin/google-chrome'):
        return '/usr/bin/google-chrome'
    else:
        return None


def read_parameters(parameters):
    # Check if parameters is not empty or None
    if parameters and isinstance(parameters, dict):
        try:
            max_oldness_seconds = parameters.get(
                "max_oldness_seconds", DEFAULT_OLDNESS_SECONDS)
        except KeyError:
            max_oldness_seconds = DEFAULT_OLDNESS_SECONDS

        try:
            maximum_items_to_collect = parameters.get(
                "maximum_items_to_collect", DEFAULT_MAXIMUM_ITEMS)
        except KeyError:
            maximum_items_to_collect = DEFAULT_MAXIMUM_ITEMS

        try:
            min_post_length = parameters.get(
                "min_post_length", DEFAULT_MIN_POST_LENGTH)
        except KeyError:
            min_post_length = DEFAULT_MIN_POST_LENGTH
    else:
        # Assign default values if parameters is empty or None
        max_oldness_seconds = DEFAULT_OLDNESS_SECONDS
        maximum_items_to_collect = DEFAULT_MAXIMUM_ITEMS
        min_post_length = DEFAULT_MIN_POST_LENGTH

    return max_oldness_seconds, maximum_items_to_collect, min_post_length


def save_cookies(driver_):
    # Save cookies
    logging.info("[Linkedin] Saving cookies.")
    pickle.dump(driver_.get_cookies(), open(COOKIE_NAME, "wb"))
    logging.info("[Linkedin] Saved cookies.")


def clear_cookies():
    """
    Clearing cookies
    """
    try:
        logging.info("[Linkedin] Clearing cookies.")
        open(COOKIE_NAME, "wb").close()
        logging.info("[Linkedin] Cleared cookies.")
    except Exception as e:
        logging.info("[Linkedin] Clear cookies error: %s", e)


def load_cookies(_driver) -> int:
    """
    Loading the cookies
    """
    try:
        logging.info("[Linkedin] Loading cookies from file")
        cookies = pickle.load(open(COOKIE_NAME, "rb"))
        logging.info("[Linkedin] Cookie file loaded")
    except:
        cookies = []
        logging.info("[Linkedin] Cookie file not found.")
        return 0

    cookies_added = 0

    for cookie in cookies:
        logging.info("\t-%s", cookie)
        # Add each cookie to the browser
        # Check if the cookie is expired
        if 'expiry' in cookie and datetime.fromtimestamp(cookie['expiry']) < datetime.now():
            logging.info("[Linkedin] Cookie expired")
        else:
            try:
                driver.add_cookie(cookie)
                cookies_added += 1
            except exceptions.InvalidCookieDomainException as e:
                logging.info("[Linkedin] Not importable cookie: %s", e)
            except:
                logging.info("[Linkedin] Error for cookie %s", cookie)
                cookies_not_imported += 1
    logging.info("[Linkedin] Imported %s cookies.", cookies_added)
    return cookies_added


def get_post_time(post_id: str) -> datetime:
    """
    Retreiving the post/comment time using the id of the URL
    """
    post_id_int = int(post_id)
    post_id_bin = bin(post_id_int)[2:43]
    post_id_dec = int(post_id_bin, 2)
    return datetime.utcfromtimestamp(post_id_dec/1000)


def login() -> bool:
    """
    Login using the credentials provided in the configurations
    """
    global driver

    sleep(random.uniform(0, 1))
    driver.get(f"https://{DOMAIN}")
    load_cookies(driver)
    driver.refresh()
    wait_page_load()

    if "authwall" in driver.current_url:
        driver.get(f"https://{DOMAIN}")
        wait_page_load()

    if "feed" in driver.current_url:  # Logged in
        return True

    wait = WebDriverWait(driver, 10)

    credentials = check_env()

    logging.info("[Linkedin] Waiting till the login form loaded")
    el = wait.until(EC.visibility_of_element_located((By.ID, "session_key")))
    logging.info("[Linkedin] Loaded the username field")
    el.send_keys(credentials["username"])
    el.send_keys(Keys.TAB)
    logging.info("[Linkedin] Waiting till the password field loaded")
    el = wait.until(EC.visibility_of_element_located(
        (By.ID, "session_password")))
    logging.info("[Linkedin] Loaded the password field")
    el.send_keys(credentials["password"])
    el.send_keys(Keys.ENTER)
    wait_page_load()
    if "feed" not in driver.current_url:
        logging.error("[Linkedin] Security confirmation")
        return False

    return True

def navigate_search_page_using_url(keyword: str):
    """
    Navigating to the search results page by directly using the URL
    """
    global driver
    date_posted = "past-24h"
    if DATE_POSTED == "Past month":
        date_posted = "past-month"
    elif DATE_POSTED == "Past week":
        date_posted = "past-week"

    sort_by = "relevance"
    if SORT_BY == "Latest":
        sort_by = "date_posted"

    url = urlunparse(("https", DOMAIN, "/search/results/content/", None,
                     f"datePosted=\"{date_posted}\"&keywords={keyword}&origin=FACETED_SEARCH&sortBy=\"{sort_by}\"", None))
    driver.get(url)

    wait_page_load()

def load_all_replies():
    """
    Loading all replie comments for loaded comments
    """
    global driver
    logging.info("[Linkedin] Loading all the replies")
    prev_reply_btns = driver.find_elements_by_class_name("show-prev-replies")
    while len(prev_reply_btns) > 0:
        for prev_reply_btn in prev_reply_btns:
            driver.execute_script(
                "arguments[0].click();", prev_reply_btn)
            sleep(random.uniform(.2, 1))
        wait_page_load()
        prev_reply_btns = driver.find_elements_by_class_name(
            "show-prev-replies")


def author_url_to_id(author_url: str) -> str:
    """
    Converting the author url to id
    """
    author_url_parsed = urlparse(author_url)
    author_url_path = author_url_parsed.path
    sha1 = hashlib.sha1()
    sha1.update(author_url_path.encode())
    return sha1.hexdigest()


def load_more_comments() -> bool:
    """
    Loading more comments to the list
    """
    global driver

    logging.info("[Linkedin] Loading more comments")
    load_link = None
    try:
        load_link = driver.find_element_by_xpath(
            "//button[.//*[contains(.,'Load more comments')]]")
    except Exception as e:
        logging.error("[Linkedin] Could not find load comments button")
        return False

    if load_link is None:
        return False

    load_link.click()
    wait_page_load()
    return True


async def scrape_comment_list(post_url: str, maximum_items_to_collect: int, min_post_length: int, max_oldness_seconds: int) -> AsyncGenerator[Item, None]:
    """
    Scraping the comment list of a post
    """
    global driver 

    wait_cpy_btn = WebDriverWait(driver, 4)

    driver.switch_to.new_window(WindowTypes.TAB)

    driver.get(post_url)
    wait_page_load()

    load_all_replies()

    comments_count = 0
    comment_id = 0
    last_comment_urn = None
    while comments_count < maximum_items_to_collect:
        comment_el = None
        try:
            comment_el = driver.find_element_by_xpath(
                f"(//*[contains(concat(' ', normalize-space(@class), ' '), ' comments-comment-item ')])[{comment_id + 1}]")
        except Exception as e:
            logging.error("[Linkedin] Could not find any new comments.")

        if comment_el is None:
            if not load_more_comments():
                break
            else:
                load_all_replies()
                continue

        class_attr = comment_el.get_attribute("class")
        is_reply = "reply" in class_attr;

        author_el = comment_el.find_element_by_xpath(
            "//a[contains(@href, '/in/') or contains(@href, '/company/')]")
        author_url = author_el.get_attribute("href")
        author_val = author_url_to_id(author_url)

        content_el = comment_el.find_element_by_class_name(
            "update-components-text")
        text_content = content_el.get_attribute("textContent")
        text_content = re.sub("( +)", " ", text_content)  # Replacing spaces
        # Replacing single lines
        text_content = re.sub(
            "[^\s|^](( +)|)\n(( +)|)([^\s]|$)", "\n", text_content)
        # Replacing multiple new lines with a single line
        text_content = re.sub(
            "([\s]+|)\n([\s]+|)\n([\s]+|)", "\n\n", text_content)
        text_content = text_content.strip()

        comment_urn = None
        try:
            logging.info("[Linkedin] Finding the comment id element")
            id_el = comment_el.find_element_by_xpath("//*[@data-id]")
            comment_urn = id_el.get_attribute("data-id")
        except Exception as e:
            logging.error("[Linkedin] Could not find the comment id element")

        if comment_urn is None:
            comment_id = comment_id + 1
            continue

        activity_id = comment_urn[25:44]

        if not is_reply:
            last_comment_urn = comment_urn

        logging.info("[Linkedin] Fetching the comment URL using the clipboard")

        comment_url = None
        if is_reply:
            comment_url = urlunparse(("https", DOMAIN, "/feed/update/urn:li:activity:"+activity_id, None,
                     f"commentUrn={last_comment_urn}&replyUrn={comment_urn}", None))
        else:
            comment_url = urlunparse(("https", DOMAIN, "/feed/update/urn:li:activity:"+activity_id, None,
                     f"commentUrn={last_comment_urn}", None))

        external_id = comment_urn[45:64]

        logging.info(
            "[Linkedin] Calculating the comment time using comment url %s", comment_url)
        comment_time: datetime = get_post_time(external_id)

        if (datetime.utcnow() - comment_time).seconds > max_oldness_seconds:
            logging.info(
                "[Linkedin] Comment excluded due to time. %s", comment_time)
            break

        logging.info("[Linkedin] Comment time calculated %s", comment_time)
        comment_timestamp = comment_time.isoformat("T") + "Z"

        item = Item(created_at=CreatedAt(comment_timestamp),
                    domain=Domain(DOMAIN),
                    url=Url(comment_url),
                    content=Content(text_content), author=Author(author_val), external_id=ExternalId(external_id))

        yield item
        comment_id = comment_id + 1
    driver.close()
    driver.switch_to.window(driver.window_handles[0])


async def scrape_post_list(list_index: int, maximum_items_to_collect: int, min_post_length: int, max_oldness_seconds: int) -> AsyncGenerator[Item, None]:
    """
    Scraping the post list
    """
    global driver 

    wait = WebDriverWait(driver, 2)
    wait_cpy_btn = WebDriverWait(driver, 4)

    post_list = None
    try:
        post_list = wait.until(EC.visibility_of_element_located(
            (By.XPATH, f"(//ul[contains(@class,'reusable-search__entity-result-list')])[{list_index + 1}]")))
    except Exception as e:
        logging.error("[Linkedin] Post list not found")
        return

    post_id = 0
    post_count = 0
    while post_count < maximum_items_to_collect:
        post = None
        try:
            logging.info(
                f"[Linkedin] Trying to find the {post_id+1}th post of the current list")
            post = post_list.find_element_by_xpath(f"(./li)[{post_id+1}]")
        except Exception as e:
            logging.error(
                f"[Linkedin] No more posts in the current list. Exception:- %s", e)

        if post is None:
            break

        logging.info("[Linkedin] Processing post #%s", post_id)

        driver.execute_script(
            "arguments[0].setAttribute('id', arguments[1]);", post, "exPost"+str(post_id))

        driver.execute_script("arguments[0].scrollIntoView(true);", post)
        sleep(random.uniform(.2, 1))

        text_content_el = None
        try:
            logging.info(
                "[Linkedin] Finding the text content element of the post")
            text_content_el = wait.until(EC.visibility_of_element_located(
                (By.CSS_SELECTOR, f"#exPost{post_id} .update-components-text")))
        except Exception as e:
            logging.error(
                "[Linkedin] Could not find the text element of the post. %s", e)

        if text_content_el is None:
            post_id = post_id + 1
            continue

        text_content = text_content_el.get_attribute("textContent")
        text_content = re.sub("( +)", " ", text_content)  # Replacing spaces
        # Replacing single lines
        text_content = re.sub(
            "[^\s|^](( +)|)\n(( +)|)([^\s]|$)", "\n", text_content)
        # Replacing multiple new lines with a single line
        text_content = re.sub(
            "([\s]+|)\n([\s]+|)\n([\s]+|)", "\n\n", text_content)
        text_content = text_content.strip()

        if len(text_content) < min_post_length:
            post_id = post_id + 1
            logging.info(
                "[Linkedin] Post excluded due to length. %s", text_content)
            continue

        author_el = None
        try:
            logging.info("[Linkedin] Finding the author name element")

            author_el = post.find_element_by_xpath(
                "//a[contains(@href, '/in/') or contains(@href, '/company/')]")
        except Exception as e:
            logging.error(
                "[Linkedin] Could not find the author name element. Exception:- %s", e)

        if author_el is None:
            post_id = post_id + 1
            continue

        author_url = author_el.get_attribute("href")
        author_id = author_url_to_id(author_url)

        urn = None
        try:
            urn_el = post.find_element_by_xpath('//*[@data-urn]')
            urn = urn_el.get_attribute("data-urn")
        except Exception as e:
            logging.error("[Linkedin] Could not find data URL")

        if urn is None:
            post_id = post_id + 1
            continue


        external_id = urn[16:]

        logging.info(
            "[Linkedin] Calculating the post time using post id %s", external_id)
        post_time: datetime = get_post_time(external_id)

        if (datetime.utcnow() - post_time).seconds > max_oldness_seconds:
            logging.info("[Linkedin] Post excluded due to time. %s", post_time)
            yield None
            break

        logging.info("[Linkedin] Post time calculated %s", post_time)
        post_timestamp = post_time.isoformat("T") + "Z"

        post_url = f"https://{DOMAIN}/feed/update/{urn}"

        item = Item(created_at=CreatedAt(post_timestamp),
                    domain=Domain(DOMAIN),
                    url=Url(post_url),
                    content=Content(text_content), author=Author(author_id), external_id=ExternalId(external_id))

        post_count = post_count + 1
        logging.info("[Linkedin] Item yielded")
        yield item

        async for comment_item in scrape_comment_list(post_url, maximum_items_to_collect-post_count, min_post_length, max_oldness_seconds):
            yield comment_item
            post_count = post_count + 1

        sleep(random.uniform(1, 2))
        post_id = post_id + 1

def load_more_items(next_list_index: int) -> bool:
    """
    Loading more items by clicking on the load more items button in search results page
    """
    global driver

    wait = WebDriverWait(driver, 2)

    try:
        logging.info("[Linkedin] Checking next list already loaded")
        post_list = wait.until(EC.visibility_of_element_located(
            (By.XPATH, f"(//ul[contains(@class,'reusable-search__entity-result-list')])[{next_list_index + 1}]")))
        return True
    except Exception as e:
        logging.info("[Linkedin] Next list not loaded yet")

    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

    wait = WebDriverWait(driver, 5)

    try:
        logging.info("[Linkedin] Checking next list loaded")
        post_list = wait.until(EC.visibility_of_element_located(
            (By.XPATH, f"(//ul[contains(@class,'reusable-search__entity-result-list')])[{next_list_index + 1}]")))
        return True
    except Exception as e:
        logging.info("[Linkedin] No any lists to load")

    return False


async def scrape(keyword: str, maximum_items_to_collect: int, min_post_length: int, max_oldness_seconds: int) -> AsyncGenerator[Item, None]:
    global driver

    try:
        check_and_kill_processes(["chromium", "chromedriver", "google-chrome"])
    except Exception as e:
        logging.info("[Linkedin] Could not kill existing processes: %s", e)

    try:
        logging.info("[Linkedin] Open driver")
        driver = init_driver(headless=True)
        logging.info("[Linkedin] Chrome Selenium Driver =  %s", driver)
    except Exception as e:
        logging.info("[Linkedin] Exception during Twitter Init:  %s", e)

    try:
        logging.info("[Linkedin] Login")
        if not (login()):
            return
    except Exception as e:
        logging.info(
            "[Linkedin] Could not login to the site. Exception: %s", e)

    try:
        logging.info("[Linkedin] Navigating to the search page using URL")
        navigate_search_page_using_url(keyword)
    except Exception as e:
        logging.info(
            "[Linkedin] Could not navigate to the search page using URL. Exception:- %s", e)

    item_count = 0
    list_index = 0
    while item_count < maximum_items_to_collect:
        time_gone = False
        async for item in scrape_post_list(list_index, maximum_items_to_collect - item_count, min_post_length, max_oldness_seconds):
            item_count = item_count + 1
            if item is None:
                time_gone = True
                break
            yield item
        if time_gone:
            logging.info("[Linkedin] Exited due to expired posts")
            break
        try:
            has_more_item = load_more_items(list_index + 1)
            if not (has_more_item):
                logging.info("[Linkedin] Do not have more items")
                break
            else:
                list_index = list_index + 1
        except Exception as e:
            logging.error(
                "[Linkedin] Could not load more items. Exception:- %s", e)
            break

    try:
        logging.info("[Linkedin] Saving the cookies")
        save_cookies(driver)
        logging.info("[Linkedin] Saved the cookies")
    except Exception as e:
        logging.error("[Linkedin] Could not save the cookies. %s", e)


async def query(parameters: dict) -> AsyncGenerator[Item, None]:
    logging.info("[Linkedin ] Scraping posts")
    max_oldness_seconds, maximum_items_to_collect, min_post_length = read_parameters(
        parameters)

    async for item in scrape(parameters["keyword"], maximum_items_to_collect, min_post_length, max_oldness_seconds):
        yield item
