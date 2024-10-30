#!/usr/bin/env python3
import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
import time
from pyvirtualdisplay import Display
from multiprocessing import Pool
import argparse

# Directories to check for existing files and chunks
existing_files_dir = '/home/aspandiyar/Downloads'
chunks_base_dir = '/home/aspandiyar/Get-data-huggingface/data/ucinlp-drop/chunks'
failed_chunks_log = '/home/aspandiyar/Get-data-huggingface/data/ucinlp-drop/chunks/failed_chunks.txt'

def setup_driver(retries=3, delay=5):
    for attempt in range(retries):
        try:
            options = webdriver.ChromeOptions()
            options.add_argument("--disable-blink-features=AutomationControlled")
            prefs = {
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True,
                "download.default_directory": existing_files_dir  # Set the download directory
            }
            options.add_experimental_option("prefs", prefs)

            driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
            driver.maximize_window()
            wait = WebDriverWait(driver, 10)
            return driver, wait
        except Exception as e:
            print(f"Error setting up WebDriver: {e}. Retrying in {delay} seconds... ({attempt + 1}/{retries})")
            time.sleep(delay)
    return None, None

def click_translate_button(driver, wait):
    try:
        translate_button_xpath = '/html/body/c-wiz/div/div[2]/c-wiz/div[3]/c-wiz/div[2]/c-wiz/div/div[1]/div/div[2]/div/div/button/div[1]'
        translate_button = wait.until(EC.element_to_be_clickable((By.XPATH, translate_button_xpath)))
        driver.execute_script("arguments[0].click();", translate_button)
    except TimeoutException as e:
        print(f"Timeout while waiting for translate button: {e}")
    except WebDriverException as e:
        print(f"WebDriverException while clicking translate button: {e}")

def click_download_button(driver, wait):
    try:
        download_button_xpath = '/html/body/c-wiz/div/div[2]/c-wiz/div[3]/c-wiz/div[2]/c-wiz/div/div[1]/div/div[2]/div/button/span[2]'
        download_button = wait.until(EC.element_to_be_clickable((By.XPATH, download_button_xpath)))
        driver.execute_script("arguments[0].click();", download_button)
    except TimeoutException as e:
        print(f"Timeout while waiting for download button: {e}")
        raise e  # Raise exception to be caught in process_file
    except WebDriverException as e:
        print(f"WebDriverException while clicking download button: {e}")
        raise e  # Raise exception to be caught in process_file

def wait_for_download(file_name, timeout=60):
    start_time = time.time()
    file_path = os.path.join(existing_files_dir, file_name)
    while not os.path.exists(file_path):
        if time.time() - start_time > timeout:
            print(f"Timeout waiting for download: {file_name}")
            return False
        time.sleep(1)
    print(f"Download completed: {file_name}")
    return True

def process_file(driver, wait, file_path, target_language):
    try:
        driver.get(f'https://translate.google.com/?sl=auto&tl={target_language}&op=docs')
        upload_input = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'input[type="file"]')))
        upload_input.send_keys(file_path)

        max_retries = 5
        retry_count = 0

        while retry_count < max_retries:
            click_translate_button(driver, wait)
            time.sleep(5)

            try:
                click_download_button(driver, wait)
                download_file_name = os.path.basename(file_path)
                if wait_for_download(download_file_name):
                    print(f"Translation completed and file downloaded: {file_path}")
                    break
            except TimeoutException:
                retry_count += 1
                print(f"Download button not available, retrying... ({retry_count}/{max_retries})")
            except WebDriverException as e:
                retry_count += 1
                print(f"WebDriverException during download button click: {e}")

        if retry_count == max_retries:
            print(f"Failed to process file: {file_path} after {max_retries} retries.")
            with open(failed_chunks_log, 'a') as log_file:
                log_file.write(f"{file_path}\n")
            return False

        time.sleep(2)
        return True
    except Exception as e:
        print(f"Exception during file processing: {e}")
        return False

def process_folder(folder_path, target_language):
    print(f"Processing folder: {folder_path}")
    files_to_process = [os.path.join(folder_path, filename) for filename in os.listdir(folder_path) if filename.endswith(".xlsx")]

    already_translated = [filename for filename in os.listdir(existing_files_dir) if filename.endswith(".xlsx")]
    files_to_process = [file for file in files_to_process if os.path.basename(file) not in already_translated]

    print(f"Total files to process in {folder_path}: {len(files_to_process)}")

    with Pool(processes=16) as pool:
        pool.starmap(main, [(file, target_language) for file in files_to_process])

def main(file_path, target_language):
    start_time = time.time()

    existing_file_path = os.path.join(existing_files_dir, os.path.basename(file_path))

    if os.path.exists(existing_file_path):
        print(f"File already exists, skipping: {existing_file_path}")
        return

    display = Display(visible=0, size=(1920, 1080))
    display.start()

    driver, wait = None, None
    try:
        driver, wait = setup_driver()
        if driver is None:
            print(f"Failed to initialize WebDriver for file: {file_path}")
            return

        print(f"Processing file: {file_path}")

        success = process_file(driver, wait, file_path, target_language)
        if not success:
            print("Reopening Chrome and retrying...")
            driver.quit()
            driver, wait = setup_driver()
            if driver is None:
                print(f"Failed to reinitialize WebDriver for file: {file_path}")
                return

            success = process_file(driver, wait, file_path, target_language)
            if not success:
                print(f"Failed to process file: {file_path} after reopening Chrome.")
    finally:
        if driver is not None:
            driver.quit()
        display.stop()

    print(f"File {file_path} processed successfully.")
    print("--- %s seconds ---" % (time.time() - start_time))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Translate files using Google Translate.')
    parser.add_argument('--target_language', type=str, required=True, help='Target language code (e.g., "kk" for Kazakh)')
    args = parser.parse_args()

    # Iterate through all folders in the chunks directory
    folders_to_process = [os.path.join(chunks_base_dir, folder) for folder in os.listdir(chunks_base_dir) if os.path.isdir(os.path.join(chunks_base_dir, folder))]

    print(f"Found {len(folders_to_process)} folders to process.")

    for folder in folders_to_process:
        process_folder(folder, args.target_language)