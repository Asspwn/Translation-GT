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
import subprocess
import random

# Adjusted paths
existing_files_dir = os.path.join(os.getcwd(), 'translated_chunks')
chunks_base_dir = os.path.join(os.getcwd(), 'chunks')
failed_chunks_log = os.path.join(existing_files_dir, 'failed_chunks.txt')

# Create directories if they don't exist
os.makedirs(existing_files_dir, exist_ok=True)

def get_free_display_number():
    """Find a free display number for Xvfb"""
    used_displays = []
    try:
        # Get currently used display numbers
        ps = subprocess.Popen(['ps', 'aux'], stdout=subprocess.PIPE)
        output = subprocess.check_output(['grep', 'Xvfb'], stdin=ps.stdout)
        ps.wait()
        
        for line in output.decode().split('\n'):
            if 'Xvfb :' in line:
                try:
                    display_num = int(line.split('Xvfb :')[1].split()[0])
                    used_displays.append(display_num)
                except:
                    continue
    except:
        pass
    
    # Find first unused display number
    display_num = random.randint(99, 999)
    while display_num in used_displays:
        display_num = random.randint(99, 999)
    
    return display_num

def setup_virtual_display():
    """Setup virtual display with proper error handling"""
    try:
        display_num = get_free_display_number()
        display = Display(visible=0, size=(1920, 1080), backend="xvfb", use_xauth=True)
        
        # Set specific display number
        os.environ['DISPLAY'] = f':{display_num}'
        display.start()
        
        return display
    except Exception as e:
        print(f"Error setting up virtual display: {e}")
        return None

def get_chrome_path():
    """Get Chrome binary path dynamically"""
    try:
        # Try to get Chrome path using 'which'
        chrome_path = subprocess.check_output(['which', 'google-chrome']).decode().strip()
        if os.path.exists(chrome_path):
            return chrome_path
    except:
        # Fallback paths
        paths = [
            '/usr/bin/google-chrome',
            '/usr/bin/google-chrome-stable',
            '~/.local/bin/google-chrome',
            '/usr/bin/chromium-browser'
        ]
        for path in paths:
            expanded_path = os.path.expanduser(path)
            if os.path.exists(expanded_path):
                return expanded_path
    return None

def setup_driver(retries=3, delay=5):
    """Setup Chrome driver with additional options for virtual display"""
    for attempt in range(retries):
        try:
            options = webdriver.ChromeOptions()
            
            # Get Chrome path dynamically
            chrome_path = get_chrome_path()
            if not chrome_path:
                raise Exception("Chrome binary not found")
            options.binary_location = chrome_path
            
            # Essential options
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-gpu')
            options.add_argument(f"--display={os.environ.get('DISPLAY', ':99')}")
            
            # JavaScript and automation related
            options.add_argument('--disable-web-security')
            options.add_argument('--allow-running-insecure-content')
            options.add_argument('--disable-blink-features=AutomationControlled')
            
            # Additional stability options
            options.add_argument('--disable-extensions')
            options.add_argument('--disable-logging')
            options.add_argument('--ignore-certificate-errors')
            options.add_argument('--disable-popup-blocking')
            
            prefs = {
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True,
                "download.default_directory": existing_files_dir,
                "profile.default_content_setting_values.notifications": 2,
                "profile.default_content_settings.popups": 0,
                "profile.default_content_setting_values.automatic_downloads": 1
            }
            options.add_experimental_option("prefs", prefs)
            options.add_experimental_option('excludeSwitches', ['enable-logging', 'enable-automation'])
            
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)
            driver.set_window_size(1920, 1080)
            
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
    files_to_process = [
        os.path.join(folder_path, filename)
        for filename in os.listdir(folder_path)
        if filename.endswith(".xlsx")
    ]

    already_translated = [
        filename for filename in os.listdir(existing_files_dir)
        if filename.endswith(".xlsx")
    ]
    files_to_process = [
        file for file in files_to_process
        if os.path.basename(file) not in already_translated
    ]

    print(f"Total files to process in {folder_path}: {len(files_to_process)}")

    with Pool(processes=30) as pool:
        pool.starmap(main, [(file, target_language) for file in files_to_process])

def main(file_path, target_language):
    start_time = time.time()

    existing_file_path = os.path.join(existing_files_dir, os.path.basename(file_path))

    if os.path.exists(existing_file_path):
        print(f"File already exists, skipping: {existing_file_path}")
        return

    # Start with display setup
    display = None
    driver = None
    
    try:
        # Set up virtual display
        display = Display(visible=0, size=(1920, 1080))
        display.start()
        
        # Set up driver
        driver, wait = setup_driver()
        if driver is None:
            print(f"Failed to initialize WebDriver for file: {file_path}")
            return

        print(f"Processing file: {file_path}")
        success = process_file(driver, wait, file_path, target_language)
        
        if not success:
            print("First attempt failed, retrying with fresh driver...")
            if driver:
                driver.quit()
            driver, wait = setup_driver()
            if driver is None:
                print(f"Failed to reinitialize WebDriver for file: {file_path}")
                return
            
            success = process_file(driver, wait, file_path, target_language)
            if not success:
                print(f"Failed to process file: {file_path} after retry.")
                
    except Exception as e:
        print(f"Unexpected error during processing: {e}")
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass
        if display:
            try:
                display.stop()
            except:
                pass

    print(f"File {file_path} processing completed in {time.time() - start_time} seconds")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Translate files using Google Translate.')
    parser.add_argument('--target_language', type=str, required=True, help='Target language code (e.g., "kk" for Kazakh)')
    args = parser.parse_args()

    # Iterate through all folders in the chunks directory
    folders_to_process = [
        os.path.join(chunks_base_dir, folder)
        for folder in os.listdir(chunks_base_dir)
        if os.path.isdir(os.path.join(chunks_base_dir, folder))
    ]

    print(f"Found {len(folders_to_process)} folders to process.")

    for folder in folders_to_process:
        process_folder(folder, args.target_language)
