import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from selenium.webdriver.common.action_chains import ActionChains
import json
import time
import traceback
import os
import shutil
import signal
import platform
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import tempfile
import psutil
import subprocess

def get_time_range_uae(delta_minutes: int = 30):
    timezone_str = "Asia/Dubai"
    now = datetime.now(tz=ZoneInfo(timezone_str))
    delta = timedelta(minutes=delta_minutes)
    start_date = now - delta
    end_date = now
    time_format = '%d-%m-%Y %I:%M%p'
    return start_date.strftime(time_format), end_date.strftime(time_format)

class TouchTraksLogin:
    def __init__(self, headless=True, save_screenshots=True):
        self.driver = None
        self.wait = None
        self.user_data_dir = None
        self.save_screenshots = save_screenshots
        self.max_retries = 3
        self.retry_delay = 2
        self.download_directories = ["idlereport", "exidlereport", "driverperformance", "travelreport", "geofence"]
        self.is_windows = platform.system().lower() == "windows"
        self.setup_with_retries(headless)

    def setup_with_retries(self, headless):
        for attempt in range(3):
            try:
                self.setup_driver(headless)
                break
            except Exception as e:
                print(f"Attempt {attempt+1} failed: {e}")
                self.close()
                time.sleep(5)
        else:
            raise RuntimeError("All attempts to start driver failed")

    def setup_driver(self, headless=False):
        print('helloya')

        local_uc_profile = os.path.join(tempfile.gettempdir(), "uc_profile")
        shutil.rmtree(local_uc_profile, ignore_errors=True)
        
        print('byeoya')

        options = uc.ChromeOptions()
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-web-security')
        options.add_argument('--disable-features=VizDisplayCompositor')
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')

        download_dir = os.path.abspath(os.curdir)

        prefs = {
            "download.default_directory": download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
            "safebrowsing.disable_download_protection": True,
        }
        options.add_experimental_option("prefs", prefs)

        self.user_data_dir = os.path.join(tempfile.gettempdir(), "chrome_user_data")
        options.add_argument(f"--user-data-dir={self.user_data_dir}")

        if headless:
            options.add_argument('--headless=new')
            options.add_argument('--window-size=1920,1080')
            options.add_argument('--disable-gpu')
        else:
            options.add_argument('--start-maximized')

        self.driver = uc.Chrome(
            options=options,
            headless=headless,
            version_main=138
        )
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        self.wait = WebDriverWait(self.driver, 20)
        self.driver.execute_script("document.body.style.zoom='50%'")
        print("Driver setup complete")

    def wait_for_downloads_complete(self, timeout=360):
        print("Waiting for all downloads to complete...")
        time.sleep(120)
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            all_complete = True
            
            for directory in self.download_directories:
                if os.path.exists(directory):
                    for file in os.listdir(directory):
                        if file.endswith('.crdownload') or file.endswith('.tmp'):
                            print(f"Still downloading in {directory}: {file}")
                            all_complete = False
                            break
            
            if all_complete:
                print("All downloads completed successfully")
                return True
                
            time.sleep(10)
        
        print(f"Download timeout reached ({timeout}s)")
        return False

    def kill_process_windows(self, pid):
        try:
            subprocess.run(['taskkill', '/F', '/PID', str(pid)], 
                         check=False, capture_output=True, text=True)
            return True
        except Exception as e:
            print(f"Failed to kill process {pid} with taskkill: {e}")
            return False

    def kill_process_unix(self, pid):
        try:
            os.kill(pid, signal.SIGTERM)
            time.sleep(2)
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            return True
        except ProcessLookupError:
            return True
        except Exception as e:
            print(f"Failed to kill process {pid}: {e}")
            return False

    def force_kill_process(self, pid):
        if self.is_windows:
            return self.kill_process_windows(pid)
        else:
            return self.kill_process_unix(pid)

    def verify_driver_cleanup(self):
        chrome_found = []
        chromedriver_found = []

        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                pname = proc.info['name'].lower()

                if 'chromedriver' in pname:
                    chromedriver_found.append(proc)
                elif 'chrome' in pname or 'chrome.exe' in pname:
                    if self.user_data_dir:
                        cmdline = proc.info.get('cmdline') or []
                        if any(self.user_data_dir in arg for arg in cmdline):
                            chrome_found.append(proc)
                    else:
                        chrome_found.append(proc)

            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                continue

        if chromedriver_found:
            print(f"Still running chromedriver processes:")
            for proc in chromedriver_found:
                try:
                    cmdline = proc.info.get('cmdline', [])
                    print(f"PID {proc.pid}, cmd: {' '.join(cmdline) if cmdline else 'N/A'}")
                except:
                    print(f"PID {proc.pid}, cmd: Unable to get command line")
        else:
            print("No chromedriver processes remain.")

        if chrome_found:
            print(f"Still running Chrome processes:")
            for proc in chrome_found:
                try:
                    cmdline = proc.info.get('cmdline', [])
                    print(f"  PID {proc.pid}, cmd: {' '.join(cmdline) if cmdline else 'N/A'}")
                except:
                    print(f"  PID {proc.pid}, cmd: Unable to get command line")
        else:
            print("No Chrome processes remain.")

        if not chromedriver_found and not chrome_found:
            print("All browser processes are fully cleaned up.")

        return len(chrome_found) == 0 and len(chromedriver_found) == 0

    def cleanup_driver_process_tree(self, timeout=120):
        if not self.driver or not hasattr(self.driver, 'service') or not self.driver.service.process:
            print("No driver process to cleanup")
            return

        try:
            driver_proc = psutil.Process(self.driver.service.process.pid)
            print(f"DEBUG: Waiting for Chrome child processes of PID {driver_proc.pid}")

            all_children = driver_proc.children(recursive=True)
            print(f"Found {len(all_children)} child processes")

            start_time = time.time()
            graceful_timeout = min(timeout // 2, 60)

            while time.time() - start_time < graceful_timeout:
                still_alive = []
                for child in all_children:
                    try:
                        if child.is_running():
                            still_alive.append(child)
                    except (psutil.NoSuchProcess, psutil.ZombieProcess):
                        continue

                if not still_alive:
                    print("All Chrome children exited gracefully.")
                    break

                for child in still_alive:
                    try:
                        if self.is_windows:
                            child.terminate()
                        else:
                            child.send_signal(signal.SIGTERM)
                    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                        continue

                time.sleep(1)
            else:
                print("Graceful termination timeout reached, proceeding to force kill...")

            remaining_children = []
            for child in all_children:
                try:
                    if child.is_running():
                        remaining_children.append(child)
                except (psutil.NoSuchProcess, psutil.ZombieProcess):
                    continue

            if remaining_children:
                print(f"Force-killing {len(remaining_children)} remaining processes...")
                for child in remaining_children:
                    try:
                        print(f"Force-killing child PID {child.pid}: {child.name()}")
                        if self.is_windows:
                            self.kill_process_windows(child.pid)
                        else:
                            child.kill()
                    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess) as e:
                        print(f"Could not kill PID {child.pid}: {e}")

            try:
                if driver_proc.is_running():
                    print(f"Killing Chrome driver PID {driver_proc.pid}")
                    if self.is_windows:
                        self.kill_process_windows(driver_proc.pid)
                    else:
                        driver_proc.kill()
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess) as e:
                print(f"Could not kill driver process: {e}")

            time.sleep(2)

        except (psutil.NoSuchProcess, psutil.AccessDenied) as e:
            print(f"Driver process already terminated: {e}")

    def cleanup_temp_directories(self):
        directories_to_clean = []
        
        if self.user_data_dir and os.path.exists(self.user_data_dir):
            directories_to_clean.append(self.user_data_dir)

        temp_chrome_dirs = [
            os.path.join(tempfile.gettempdir(), "uc_profile"),
            os.path.join(tempfile.gettempdir(), "chrome_temp"),
        ]
        
        for temp_dir in temp_chrome_dirs:
            if os.path.exists(temp_dir):
                directories_to_clean.append(temp_dir)

        for directory in directories_to_clean:
            max_attempts = 3
            for attempt in range(max_attempts):
                try:
                    if os.path.exists(directory):
                        if self.is_windows:
                            for root, dirs, files in os.walk(directory, topdown=False):
                                for file in files:
                                    file_path = os.path.join(root, file)
                                    try:
                                        os.chmod(file_path, 0o777)
                                        os.remove(file_path)
                                    except (PermissionError, FileNotFoundError):
                                        pass
                                for dir in dirs:
                                    dir_path = os.path.join(root, dir)
                                    try:
                                        os.chmod(dir_path, 0o777)
                                        os.rmdir(dir_path)
                                    except (PermissionError, FileNotFoundError, OSError):
                                        pass
                            try:
                                os.rmdir(directory)
                            except (PermissionError, FileNotFoundError, OSError):
                                pass
                        else:
                            shutil.rmtree(directory, ignore_errors=True)
                        
                        if not os.path.exists(directory):
                            print(f"Successfully cleaned up temporary directory: {directory}")
                            break
                        else:
                            print(f"Attempt {attempt + 1}: Some files in {directory} could not be removed")
                            
                except Exception as e:
                    print(f"Attempt {attempt + 1} failed to clean {directory}: {e}")
                
                if attempt < max_attempts - 1:
                    time.sleep(2)
            else:
                print(f"Warning: Could not fully clean up {directory} after {max_attempts} attempts")

    def close(self):
        print("Starting browser cleanup process...")

        self.wait_for_downloads_complete()
        
        if self.driver:
            try:
                try:
                    for handle in self.driver.window_handles:
                        self.driver.switch_to.window(handle)
                        self.driver.close()
                except:
                    pass

                self.cleanup_driver_process_tree()

                try:
                    self.driver.quit()
                except WebDriverException as e:
                    print(f"WebDriver quit error (expected): {e}")

                time.sleep(3)
                
            except Exception as e:
                print(f"Error during driver cleanup: {e}")

        self.cleanup_temp_directories()
        
        print(f"CLEANED SELF.USER_DATA_DIR {self.user_data_dir}")

        cleanup_successful = self.verify_driver_cleanup()

        if not cleanup_successful:
            print("Performing aggressive cleanup...")
            time.sleep(2)

            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    pname = proc.info['name'].lower()
                    if ('chrome' in pname or 'chromedriver' in pname):
                        cmdline = proc.info.get('cmdline', [])
                        if self.user_data_dir and any(self.user_data_dir in arg for arg in cmdline):
                            print(f"Force killing remaining process PID {proc.pid}")
                            self.force_kill_process(proc.pid)
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    continue
            
            time.sleep(2)
            self.verify_driver_cleanup()

        self.driver = None
        print("Browser cleanup completed.")

    def safe_click(self, element_or_locator, timeout=10, retries=3):
        for attempt in range(retries):
            try:
                if isinstance(element_or_locator, tuple):
                    element = WebDriverWait(self.driver, timeout).until(
                        EC.element_to_be_clickable(element_or_locator)
                    )
                else:
                    element = element_or_locator
                
                self.driver.execute_script("arguments[0].scrollIntoView(true);", element)
                time.sleep(0.5)
                
                try:
                    element.click()
                except:
                    self.driver.execute_script("arguments[0].click();", element)
                
                return True
            except Exception as e:
                print(f"Click attempt {attempt + 1} failed: {e}")
                if attempt < retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    return False
        return False
    
    def safe_send_keys(self, element_or_locator, text, timeout=10, clear_first=True):
        try:
            if isinstance(element_or_locator, tuple):
                element = WebDriverWait(self.driver, timeout).until(
                    EC.visibility_of_element_located(element_or_locator)
                )
            else:
                element = element_or_locator
            
            if clear_first:
                element.clear()
            element.send_keys(text)
            return True
        except Exception as e:
            print(f"Failed to send keys: {e}")
            return False
    
    def wait_for_element(self, locator, timeout=20, condition=EC.presence_of_element_located):
        try:
            return WebDriverWait(self.driver, timeout).until(condition(locator))
        except TimeoutException:
            return None
    
    def wait_for_elements(self, locator, timeout=20):
        try:
            return WebDriverWait(self.driver, timeout).until(
                EC.presence_of_all_elements_located(locator)
            )
        except TimeoutException:
            return []
    
    def wait_for_page_load(self, timeout=60):
        try:
            WebDriverWait(self.driver, timeout).until(
                lambda driver: driver.execute_script("return document.readyState") == "complete"
            )
            time.sleep(1)
            return True
        except TimeoutException:
            return False
    
    def save_screenshot(self, name, prefix="touchtraks"):
        if not self.save_screenshots:
            return
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{prefix}_{name}_{timestamp}.png"
        
        try:
            self.driver.save_screenshot(filename)
            print(f"Screenshot saved: {filename}")
            return filename
        except Exception as e:
            print(f"Failed to save screenshot: {e}")
            return None
    
    def login(self, username="unknown", password="unknown"):
        for attempt in range(self.max_retries):
            try:
                print(f"STARTING TOUCHTRAKS LOGIN PROCESS - Attempt {attempt + 1}")

                print("Step 1: Loading TouchTraks website...")
                self.driver.get("http://www.touchtraks.com")
                
                if not self.wait_for_page_load():
                    print("Page load timeout")
                    continue

                print(f"Current URL: {self.driver.current_url}")
                print(f"Page Title: {self.driver.title}")

                print("\nStep 2: Looking for login button...")
                login_button = self.wait_for_element(
                    (By.CSS_SELECTOR, "#slide-navbar-collapse > ul.nav.navbar-nav.navbar-right > li > a"),
                    condition=EC.element_to_be_clickable
                )
                
                if not login_button:
                    print("Failed to find login button")
                    continue

                print("Step 3: Clicking login button...")
                if not self.safe_click(login_button):
                    print("Failed to click login button")
                    continue

                time.sleep(5)

                print("\nStep 4: Waiting for login form...")
                username_input = self.wait_for_element(
                    (By.CSS_SELECTOR, "#user_name"),
                    condition=EC.visibility_of_element_located
                )
                
                password_input = self.wait_for_element(
                    (By.CSS_SELECTOR, "#user_password"),
                    condition=EC.visibility_of_element_located
                )

                if not username_input or not password_input:
                    print("Could not find login form fields")
                    continue

                print("\nStep 5: Filling login credentials...")
                if not self.safe_send_keys(username_input, username):
                    print("Failed to enter username")
                    continue
                print(f"Entered username: {username}")

                if not self.safe_send_keys(password_input, password):
                    print("Failed to enter password")
                    continue
                print("Entered password")

                print("\nStep 6: Submitting login form...")
                submit_button = self.wait_for_element(
                    (By.CSS_SELECTOR, "#loginBtn"),
                    condition=EC.element_to_be_clickable
                )
                
                if submit_button:
                    if not self.safe_click(submit_button):
                        print("Failed to click submit button, trying Enter key...")
                        password_input.send_keys(Keys.RETURN)
                else:
                    print("No submit button found, pressing Enter...")
                    password_input.send_keys(Keys.RETURN)

                print("\nStep 7: Waiting for login response...")
                time.sleep(3)

                success = self.verify_login_success()
                
                if success:
                    print("LOGIN SUCCESSFUL!")
                    print(f"Current URL: {self.driver.current_url}")
                    return True
                else:
                    print(f"LOGIN FAILED - Attempt {attempt + 1}")
                    if attempt < self.max_retries - 1:
                        print("Retrying...")
                        time.sleep(self.retry_delay)
                    
            except Exception as e:
                print(f"Login error on attempt {attempt + 1}: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
        
        print("All login attempts failed")
        return False
    
    def verify_login_success(self):
        try:
            current_url = self.driver.current_url
            print(f"Current URL after login: {current_url}")

            success_indicators = [
                (By.CSS_SELECTOR, "div.tile-secondary-content"),
                (By.CSS_SELECTOR, "#inner-logo > div > a > i"),
                (By.CSS_SELECTOR, "#Screen3Class > div > div.box.blue-box.tracking > a > div.tile-secondary-content")
            ]
            
            element_found = False
            for selector_type, selector in success_indicators:
                element = self.wait_for_element((selector_type, selector), timeout=10)
                if element:
                    print(f"Found success indicator: {selector}")
                    element_found = True
                    break

            if element_found:
                patch_element = self.wait_for_element(
                    (By.CSS_SELECTOR, "#Screen3Class > div > div.box.blue-box.tracking > a > div.tile-secondary-content"),
                    timeout=5
                )
                if patch_element and self.safe_click(patch_element):
                    print("Criteria for login met, login successful. edit, applied patch click")
                    return True
                else:
                    print("Found success indicator but patch click failed")
                    return False

            print("Unable to definitively determine login status")
            return False
            
        except Exception as e:
            print(f"Error verifying login: {e}")
            return False
    
    def get_current_session_info(self):
        try:
            return {
                'url': self.driver.current_url,
                'title': self.driver.title,
                'cookies': len(self.driver.get_cookies()),
                'session_storage_keys': len(self.driver.execute_script("return Object.keys(sessionStorage)"))
            }
        except:
            return None
    
    def handle_logout(self, selectors):
        tracking_element = self.wait_for_element(
            (By.CSS_SELECTOR, selectors["tracking"]),
            condition=EC.presence_of_element_located
        )
        if not tracking_element or not self.safe_click(tracking_element):
            print("Failed to click tracking element")
            return False
        print("Clicked tracking successfully.")

        home_button = self.wait_for_element(
            (By.CSS_SELECTOR, selectors["home_page"]),
            condition=EC.element_to_be_clickable
        )
        if not home_button or not self.safe_click(home_button):
            print("Failed to go to home page")
            return False
        print("Went to home successfully")
        
        logout_button = self.wait_for_element(
            (By.CSS_SELECTOR, selectors["logout"]),
            condition=EC.element_to_be_clickable
        )
        if not logout_button or not self.safe_click(logout_button):
            print("Failed to click logout button")
            return False
        
        return False
    
    def handle_report(self, selectors,start_date,end_date):
        self.driver.execute_cdp_cmd("Page.setDownloadBehavior", {
            "behavior": "allow",
            "downloadPath": os.path.abspath(os.path.join(os.curdir, "data/travelreport"))
        })
        tracking_element = self.wait_for_element(
            (By.CSS_SELECTOR, selectors["tracking"]),
            timeout = 60,
            condition=EC.presence_of_element_located
        )
        if not tracking_element or not self.safe_click(tracking_element):
            print("Failed to click tracking element")
            return False
        print("Went to tracking page to fetch report")
        time.sleep(10)
        report_page_element = self.wait_for_element(
            (By.CSS_SELECTOR, selectors["report_page"]),
            timeout = 60,
            condition=EC.visibility_of_element_located
        )
        if not report_page_element or not self.safe_click(report_page_element):
            print("Failed to click report page element")
            return False
        
        dropdown_trigger = self.wait_for_element(
            (By.XPATH, "/html/body/div[3]/div/div[3]/div/div/div[2]/div/div/div[1]/div[1]/div/div/table[2]/tbody/tr/td/table/tbody/tr/td[2]/div"),
            condition=EC.element_to_be_clickable
        )
        if not dropdown_trigger or not self.safe_click(dropdown_trigger):
            print("Failed to click dropdown trigger")
            return False
        ul_element = None
        for div_num in range(10, 25):  # Check divs 10-19
            try:
                tempXpath = f"/html/body/div[{div_num}]/div/ul"
                temp_element = self.wait_for_element(
                    (By.XPATH, tempXpath),
                    timeout=0,
                    condition=EC.element_to_be_clickable
                )
                if temp_element:
                    ul_element = temp_element
                    print(f"Found dropdown in div[{div_num}]")
                    break
            except:
                continue
        if not ul_element:
            print("Failed to find dropdown list")
            return False
        
        print("Found dropdown list")
        list_items = ul_element.find_elements(By.TAG_NAME, "li")

        for i, item in enumerate(list_items):
            try:
                value = item.text.strip()
                print(f"Processing item: {value} at index {i + 1}")
                
                if i > 0:
                    if not self.safe_click((By.XPATH, "/html/body/div[3]/div/div[3]/div/div/div[2]/div/div/div[1]/div[1]/div/div/table[2]/tbody/tr/td/table/tbody/tr/td[2]/div")):
                        print(f"Failed to open dropdown for item {i + 1}")
                        continue
                
                # Find the correct dropdown div dynamically
                filter_field = None
                for div_num in range(10, 20):  # Check divs 10-19
                    try:
                        xpath = f"/html/body/div[{div_num}]/div/ul/li[{i + 1}]"
                        temp_element = self.wait_for_element(
                            (By.XPATH, xpath),
                            timeout=0,
                            condition=EC.element_to_be_clickable
                        )
                        if temp_element:
                            filter_field = temp_element
                            print(f"Found dropdown in div[{div_num}] for item {i + 1}")
                            break
                    except:
                        continue
                if not filter_field or not self.safe_click(filter_field):
                    print(f"Failed to click filter field for item {i + 1}")
                    continue
                
                fetch_button = self.wait_for_element(
                    (By.XPATH, "/html/body/div[3]/div/div[3]/div/div/div[2]/div/div/div[1]/div[1]/div/div/a[1]/span/span/span[1]"),
                    condition=EC.element_to_be_clickable
                )

                if_start_date_field_updated = self.safe_send_keys(self.driver.find_element(By.CSS_SELECTOR,'#textfield-1418-inputEl'),start_date)
                if (if_start_date_field_updated):
                    print('successfully inputed start date')
                else:
                    print('failed in inputing start date')
                if_end_date_field_updated = self.safe_send_keys(self.driver.find_element(By.CSS_SELECTOR,'#textfield-1420-inputEl'),end_date)
                if (if_end_date_field_updated):
                    print('successfully inputed end date')
                else:
                    print('failed in inputing end date')

                if not fetch_button or not self.safe_click(fetch_button):
                    print(f"Failed to click fetch button for item {i + 1}")
                    continue
                
                WebDriverWait(self.driver, 30).until(
                    EC.invisibility_of_element_located((By.CSS_SELECTOR, "[id^='loadmask-'][id$='-msgTextEl']"))
                )
                
                export_button = self.wait_for_element(
                    (By.XPATH, "/html/body/div[3]/div/div[3]/div/div/div[2]/div/div/div[1]/div[1]/div/div/a[2]/span/span/span[2]"),
                    condition=EC.element_to_be_clickable
                )
                if not export_button or not self.safe_click(export_button):
                    print(f"Failed to click export button for item {i + 1}")
                    continue
                print("Clicked export")

                export_xml_button = self.wait_for_element(
                    (By.CSS_SELECTOR, "#menuitem-1504-itemEl"),
                    condition=EC.element_to_be_clickable
                )
                if not export_xml_button or not self.safe_click(export_xml_button):
                    print(f"Failed to click XML export for item {i + 1}")
                    continue
                print("Clicked to export in xml")
                print(f"Processed item: {value}")
                
            except Exception as e:
                print(f"Error processing item {i + 1}: {e}")
                continue
        return True

    def handle_performance(self,selectors,start_date,end_date):

        self.driver.execute_cdp_cmd("Page.setDownloadBehavior", {
            "behavior": "allow",
            "downloadPath": os.path.abspath(os.path.join(os.curdir, "data/driverperformance"))
        })

        report_menu = self.wait_for_element(
            (By.XPATH,"/html/body/div[3]/div/div[3]/div/div/div[1]/div[1]/div[2]/div/a/span/span/span[1]"),
            condition = EC.element_to_be_clickable
        )
        report_menu.click()
        print("clicked report_menu")

        actions = ActionChains(self.driver)

        preceding_element = self.wait_for_element(
            (By.CSS_SELECTOR, "#menuitem-3556-textEl"),
            timeout=5,
            condition=EC.presence_of_element_located
        )

        actions.move_to_element(preceding_element).perform()
        print("Hovered on performance report options.")

        performance_report = self.wait_for_element(
            (By.CSS_SELECTOR,"#menuitem-3558-textEl"),
            timeout = 0,
            condition = EC.element_to_be_clickable
        )

        performance_report.click()

        select_all = self.driver.find_element(By.CSS_SELECTOR,'#checkbox-2481-inputEl')
        select_all.click()
        print("select all check box clicked.")
        if_start_date_field_updated = self.safe_send_keys(self.driver.find_element(By.CSS_SELECTOR,'#textfield-2483-inputEl'),start_date)
        if (if_start_date_field_updated):
            print('successfully inputed start date')
        else:
            print('failed in inputing start date')
        if_end_date_field_updated = self.safe_send_keys(self.driver.find_element(By.CSS_SELECTOR,'#textfield-2485-inputEl'),end_date)
        if (if_end_date_field_updated):
            print('successfully inputed end date')
        else:
            print('failed in inputing end date')
        fetch_button = self.wait_for_element(
            (By.CSS_SELECTOR, "#button-2538-btnInnerEl"),
            condition=EC.element_to_be_clickable
        )
        if not fetch_button or not self.safe_click(fetch_button):
            print(f"Failed to click fetch button for performance report")
        
        WebDriverWait(self.driver, 1500).until(
            EC.invisibility_of_element_located((By.CSS_SELECTOR, "[id^='loadmask-'][id$='-msgTextEl']"))
        )
        
        export_button = self.wait_for_element(
            (By.CSS_SELECTOR, "#button-2540-btnInnerEl"),
            condition=EC.element_to_be_clickable
        )
        if not export_button or not self.safe_click(export_button):
            print(f"Failed to click export button for item performance report")
        print("Clicked export")

        export_xml_button = self.wait_for_element(
            (By.CSS_SELECTOR, "#menuitem-2544-itemEl"),
            condition=EC.element_to_be_clickable
        )
        if not export_xml_button or not self.safe_click(export_xml_button):
            print(f"Failed to click XML export for performance report")

        print("Clicked to export in xml")
        return True
    def handle_geofence(self,selectors,start_date,end_date):

        self.driver.execute_cdp_cmd("Page.setDownloadBehavior", {
            "behavior": "allow",
            "downloadPath": os.path.abspath(os.path.join(os.curdir, "data/geofence"))
        })

        report_menu = self.wait_for_element(
            (By.XPATH,"/html/body/div[3]/div/div[3]/div/div/div[1]/div[1]/div[2]/div/a/span/span/span[1]"),
            condition = EC.element_to_be_clickable
        )
        report_menu.click()
        print("clicked report_menu")

        actions = ActionChains(self.driver)

        preceding_element = self.wait_for_element(
            (By.CSS_SELECTOR, "#menuitem-3550-textEl"),
            timeout=5,
            condition=EC.presence_of_element_located
        )
        actions.move_to_element(preceding_element).perform()
        print("Hovered to geofence options.")

        geofence_report = self.wait_for_element(
            (By.CSS_SELECTOR,"#menuitem-3552-textEl"),
            timeout = 0,
            condition = EC.element_to_be_clickable
        )

        geofence_report.click()
        print("Clicked geofence_report_menu")
        select_all = self.driver.find_element(By.CSS_SELECTOR,'#checkbox-1834-inputEl')
        select_all.click()
        print("select all 1 check box clicked.")
        select_all = self.driver.find_element(By.CSS_SELECTOR,'#checkbox-1837-inputEl')
        select_all.click()
        print("select all 2 check box clicked.")
        if_start_date_field_updated = self.safe_send_keys(self.driver.find_element(By.CSS_SELECTOR,'#textfield-1839-inputEl'),start_date)
        if (if_start_date_field_updated):
            print('successfully inputed start date')
        else:
            print('failed in inputing start date')
        if_end_date_field_updated = self.safe_send_keys(self.driver.find_element(By.CSS_SELECTOR,'#textfield-1841-inputEl'),end_date)
        if (if_end_date_field_updated):
            print('successfully inputed end date')
        else:
            print('failed in inputing end date')
        fetch_button = self.wait_for_element(
            (By.CSS_SELECTOR, "#button-1890-btnInnerEl"),
            condition=EC.element_to_be_clickable
        )
        if not fetch_button or not self.safe_click(fetch_button):
            print(f"Failed to click fetch button for performance report")
        
        WebDriverWait(self.driver, 1500).until(
            EC.invisibility_of_element_located((By.CSS_SELECTOR, "[id^='loadmask-'][id$='-msgTextEl']"))
        )
        
        export_button = self.wait_for_element(
            (By.CSS_SELECTOR, "#button-1892-btnInnerEl"),
            condition=EC.element_to_be_clickable
        )
        if not export_button or not self.safe_click(export_button):
            print(f"Failed to click export button for geofence report")
            return False
        print("Clicked export")

        export_excel_button = self.wait_for_element(
            (By.CSS_SELECTOR, "#menuitem-1894-textEl"),
            condition=EC.element_to_be_clickable
        )
        if not export_excel_button or not self.safe_click(export_excel_button):
            print(f"Failed to download geofence report")
            return False

        print("Downloading geofence report")

        self.driver.find_element(By.CSS_SELECTOR,selectors['home_page']).click()

        return True
    def handle_idle(self,selectors,start_date,end_date):

        self.driver.execute_cdp_cmd("Page.setDownloadBehavior", {
            "behavior": "allow",
            "downloadPath": os.path.abspath(os.path.join(os.curdir, "data/idlereport"))
        })

        report_menu = self.wait_for_element(
            (By.XPATH,"/html/body/div[3]/div/div[3]/div/div/div[1]/div[1]/div[2]/div/a/span/span/span[1]"),
            condition = EC.element_to_be_clickable
        )
        report_menu.click()
        print("clicked report_menu")

        actions = ActionChains(self.driver)

        preceding_element = self.wait_for_element(
            (By.CSS_SELECTOR, "#menuitem-3523-textEl"),
            timeout=5,
            condition=EC.presence_of_element_located
        )
        actions.move_to_element(preceding_element).perform()
        print("Hovered on idle report options.")
        idle_report = self.wait_for_element(
            (By.CSS_SELECTOR,"#menuitem-3531-itemEl"),
            timeout = 0,
            condition = EC.element_to_be_clickable
        )

        idle_report.click()
        print("Clicked performance_report_menu")
        select_all = self.driver.find_element(By.CSS_SELECTOR,'#checkbox-1668-inputEl')
        select_all.click()
        print("select all check box clicked.")
        if_start_date_field_updated = self.safe_send_keys(self.driver.find_element(By.CSS_SELECTOR,'#textfield-1670-inputEl'),start_date)
        if (if_start_date_field_updated):
            print('successfully inputed start date')
        else:
            print('failed in inputing start date')
        if_end_date_field_updated = self.safe_send_keys(self.driver.find_element(By.CSS_SELECTOR,'#textfield-1672-inputEl'),end_date)
        if (if_end_date_field_updated):
            print('successfully inputed end date')
        else:
            print('failed in inputing end date')
        fetch_button = self.wait_for_element(
            (By.CSS_SELECTOR, "#button-1680-btnInnerEl"),
            condition=EC.element_to_be_clickable
        )
        if not fetch_button or not self.safe_click(fetch_button):
            print(f"Failed to click fetch button for performance report")
        
        WebDriverWait(self.driver, 1500).until(
            EC.invisibility_of_element_located((By.CSS_SELECTOR, "[id^='loadmask-'][id$='-msgTextEl']"))
        )
        
        export_button = self.wait_for_element(
            (By.CSS_SELECTOR, "#button-1681-btnInnerEl"),
            condition=EC.element_to_be_clickable
        )
        if not export_button or not self.safe_click(export_button):
            print(f"Failed to click export button for item performance report")
        print("Clicked export")

        export_xml_button = self.wait_for_element(
            (By.CSS_SELECTOR, "#menuitem-1683-textEl"),
            condition=EC.element_to_be_clickable
        )
        export_xml_button.click()

        print("Clicked to export in xml")

        return True
    def handle_exidle(self,selectors,start_date,end_date):

        self.driver.execute_cdp_cmd("Page.setDownloadBehavior", {
            "behavior": "allow",
            "downloadPath": os.path.abspath(os.path.join(os.curdir, "data/exidlereport"))
        })

        report_menu = self.wait_for_element(
            (By.XPATH,"/html/body/div[3]/div/div[3]/div/div/div[1]/div[1]/div[2]/div/a/span/span/span[1]"),
            condition = EC.element_to_be_clickable
        )
        report_menu.click()
        print("clicked report_menu")

        actions = ActionChains(self.driver)

        preceding_element = self.wait_for_element(
            (By.CSS_SELECTOR, "#menuitem-3523-textEl"),
            timeout=5,
            condition=EC.presence_of_element_located
        )
        actions.move_to_element(preceding_element).perform()
        print("Hovered on exidle report options.")
        exidle_report = self.wait_for_element(
            (By.CSS_SELECTOR,"#menuitem-3532"),
            timeout = 0,
            condition = EC.element_to_be_clickable
        )

        exidle_report.click()
        print("Clicked exidle menu.")
        select_all = self.driver.find_element(By.CSS_SELECTOR,'#checkbox-1695-inputEl')
        select_all.click()
        print("select all check box clicked.")
        if_start_date_field_updated = self.safe_send_keys(self.driver.find_element(By.CSS_SELECTOR,'#textfield-1698-inputEl'),start_date)
        if (if_start_date_field_updated):
            print('successfully inputed start date')
        else:
            print('failed in inputing start date')
        if_end_date_field_updated = self.safe_send_keys(self.driver.find_element(By.CSS_SELECTOR,'#textfield-1700-inputEl'),end_date)
        if (if_end_date_field_updated):
            print('successfully inputed end date')
        else:
            print('failed in inputing end date')
        fetch_button = self.wait_for_element(
            (By.CSS_SELECTOR, "#button-1711-btnInnerEl"),
            condition=EC.element_to_be_clickable
        )
        if not fetch_button or not self.safe_click(fetch_button):
            print(f"Failed to click fetch button for performance report")
        
        WebDriverWait(self.driver, 1500).until(
            EC.invisibility_of_element_located((By.CSS_SELECTOR, "[id^='loadmask-'][id$='-msgTextEl']"))
        )
        
        export_button = self.wait_for_element(
            (By.CSS_SELECTOR, "#button-1712-btnInnerEl"),
            condition=EC.element_to_be_clickable
        )
        if not export_button or not self.safe_click(export_button):
            print(f"Failed to click export button for item performance report")
        print("Clicked export")

        export_xml_button = self.wait_for_element(
            (By.CSS_SELECTOR, "#menuitem-1714-itemEl"),
            condition=EC.element_to_be_clickable
        )
        if not export_xml_button or not self.safe_click(export_xml_button):
            print(f"Failed to click XML export for idlereport")

        print("Clicked to export in xml")
        return True
    def keep_session_alive(self, selectors, request=None,start_date = None,end_date = None):
        print("\n" + "="*60)
        print("SESSION READY FOR FURTHER OPERATIONS")
        print("="*60)
        
        session_info = self.get_current_session_info()
        if session_info:
            print(f"Current URL: {session_info['url']}")
            print(f"Page Title: {session_info['title']}")
            print(f"Cookies: {session_info['cookies']}")
            print(f"Session Storage Keys: {session_info['session_storage_keys']}")
        
        if request is None:
            print("The browser will remain open for you to perform additional operations.")
            return True

        try:
            if request == "logout":
                return self.handle_logout(selectors)
            elif request == "report":
                return self.handle_report(selectors,start_date,end_date)
            elif request == "performance":
                return self.handle_performance(selectors,start_date,end_date)
            elif request == "geofence":
                return self.handle_geofence(selectors,start_date,end_date)
            elif request == "idlereport":
                return self.handle_idle(selectors,start_date,end_date)
            elif request == "exidlereport":
                return self.handle_exidle(selectors,start_date,end_date)
        except Exception as e:
            print(f"Error handling request '{request}': {e}")
            return False
        
        return True
def extract_all_data():
    touchtraks = None
    css_selectors = {
        "logout": "#inner-logo > div > a.logout2 > i",
        "tracking": "#Screen4Class > div > div.box.blue-box.live-tracking > a > div.tile-secondary-content > p",
        "home_page": "#container-1383-innerCt > a.backLink.back-link",
        "report_page": "#btn_Reports-btnIconEl"
    }

    try:
        touchtraks = TouchTraksLogin(headless = True,save_screenshots = False)
        with open("config_data/credentials.json", "r") as file:
            data = json.load(file)
        
        success = touchtraks.login(data["username"], data["password"])
        requests = ["report","performance","idlereport","exidlereport","geofence"]
        if success:
            for request in requests:
                seive_time = get_time_range_uae(60*24*30*2)
                if not touchtraks.keep_session_alive(css_selectors, request,seive_time[0],seive_time[1]):
                    print(f"Failed to process request, {request} halting further action.")
                    break
                time.sleep(0)
            else:
                print("\nLogout successful")
        else:
            print("\nLogin failed, cannot proceed with data extraction.")
    
    except KeyboardInterrupt:
        print("\nProcess interrupted by user.")
    except (FileNotFoundError,json.JSONDecodeError) as e:
        print(f"Configuration error: {e}")
    except Exception as e:
        print(f"An unexpected error occured in the main process: {e}")
        traceback.print_exc()
    
    finally:
        if touchtraks:
            print("All fetching done, waiting for downloads to finish.")
            touchtraks.close()
        else:
            print("touch traks object was not created no cleanup needed.")