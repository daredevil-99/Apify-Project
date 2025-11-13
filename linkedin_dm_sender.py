"""
FREE LinkedIn DM Sender using Selenium (No Apify required!)
This uses your actual browser session to send messages
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
import time
import random
from dotenv import load_dotenv

load_dotenv()


def setup_driver():
    """Setup Chrome driver - Opens a fresh browser (you'll need to login)"""
    options = webdriver.ChromeOptions()
    
    # Don't use existing profile (causes crashes if Chrome is open)
    # Instead, manually login once when browser opens
    
    # Optional: Start maximized
    options.add_argument("--start-maximized")
    
    # Disable automation flags (helps avoid detection)
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    # Keep browser open for debugging
    options.add_experimental_option("detach", True)
    
    print("\n⚠️  IMPORTANT: Browser will open. Please login to LinkedIn manually!")
    print("    After logging in, press Enter here to continue...\n")
    
    driver = webdriver.Chrome(options=options)
    
    # Go to LinkedIn and wait for manual login
    driver.get("https://www.linkedin.com/login")
    
    input("✅ Press Enter after you've logged into LinkedIn...")
    
    return driver


def send_linkedin_dm_selenium(driver, profile_url: str, message: str):
    """
    Send LinkedIn DM using Selenium
    
    Returns:
        dict: {"status": "success" | "failed", "message_sent": bool, "error": str}
    """
    try:
        print(f"🚀 Opening profile: {profile_url}")
        driver.get(profile_url)
        
        # Wait for page to load
        time.sleep(4)
        
        # Click "Message" button - try multiple selectors
        try:
            print("🔍 Looking for Message button...")
            
            # Try different possible selectors
            message_button_selectors = [
                "//button[contains(@class, 'message') and contains(@class, 'pvs-profile-actions')]",
                "//button[contains(., 'Message')]",
                "//button[@aria-label='Message']",
                "//a[contains(@href, '/messaging/')]"
            ]
            
            message_button = None
            for selector in message_button_selectors:
                try:
                    message_button = WebDriverWait(driver, 5).until(
                        EC.element_to_be_clickable((By.XPATH, selector))
                    )
                    break
                except:
                    continue
            
            if not message_button:
                return {
                    "status": "failed",
                    "message_sent": False,
                    "error": "Message button not found - are you connected with this person?"
                }
            
            message_button.click()
            print("✅ Clicked Message button")
            
        except Exception as e:
            print(f"❌ Could not find Message button: {e}")
            return {
                "status": "failed",
                "message_sent": False,
                "error": "Message button not found - are you connected with this person?"
            }
        
        # Wait for message box to appear
        time.sleep(3)
        
        # Find the message input box
        try:
            print("🔍 Looking for message input box...")
            
            # Try different selectors for the message box
            message_box_selectors = [
                "//div[@role='textbox' and @contenteditable='true']",
                "//div[contains(@class, 'msg-form__contenteditable')]",
                "//div[@aria-label='Write a message…']"
            ]
            
            message_box = None
            for selector in message_box_selectors:
                try:
                    message_box = WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.XPATH, selector))
                    )
                    break
                except:
                    continue
            
            if not message_box:
                return {
                    "status": "failed",
                    "message_sent": False,
                    "error": "Could not find message input box"
                }
            
            # Click on the message box first
            message_box.click()
            time.sleep(1)
            
            # Type the message
            message_box.send_keys(message)
            print("✅ Typed message")
            
            time.sleep(2)
            
            # Find and click the Send button (safer than pressing Enter)
            try:
                send_button = driver.find_element(By.XPATH, "//button[contains(@class, 'msg-form__send-button') or contains(., 'Send')]")
                send_button.click()
                print("✅ Clicked Send button")
            except:
                # Fallback: Press Enter
                message_box.send_keys(Keys.RETURN)
                print("✅ Pressed Enter to send")
            
            time.sleep(3)
            
            return {
                "status": "success",
                "message_sent": True,
                "platform": "linkedin",
                "profile_url": profile_url
            }
            
        except Exception as e:
            print(f"❌ Error typing/sending message: {e}")
            return {
                "status": "failed",
                "message_sent": False,
                "error": f"Could not send message: {e}"
            }
            
    except Exception as e:
        print(f"❌ General error: {e}")
        import traceback
        traceback.print_exc()
        return {
            "status": "error",
            "message_sent": False,
            "error": str(e)
        }


def process_linkedin_dms_selenium(prospects):
    """
    Process multiple LinkedIn DMs using Selenium
    """
    driver = setup_driver()
    
    successful = 0
    failed = 0
    results = []
    
    try:
        print(f"\n🚀 Processing {len(prospects)} LinkedIn DM(s)...\n")
        
        for i, prospect in enumerate(prospects, 1):
            profile_url = prospect.get("profile_url") or prospect.get("url")
            message = prospect.get("message")
            prospect_name = prospect.get("name", "Unknown")
            
            if not profile_url or not message:
                print(f"⏭️  [{i}/{len(prospects)}] Skipping {prospect_name} - missing data")
                failed += 1
                continue
            
            print(f"\n{'='*60}")
            print(f"📤 [{i}/{len(prospects)}] Sending to: {prospect_name}")
            print(f"🔗 Profile: {profile_url}")
            print(f"{'='*60}\n")
            
            # Send DM
            result = send_linkedin_dm_selenium(driver, profile_url, message)
            
            if result["status"] == "success" and result["message_sent"]:
                print(f"✅ Message sent successfully to {prospect_name}!")
                successful += 1
                results.append({
                    "name": prospect_name,
                    "profile_url": profile_url,
                    "status": "success"
                })
            else:
                print(f"❌ Failed to send to {prospect_name}: {result.get('error', 'Unknown error')}")
                failed += 1
                results.append({
                    "name": prospect_name,
                    "profile_url": profile_url,
                    "status": "failed",
                    "error": result.get('error')
                })
            
            # Wait between messages to avoid rate limiting
            if i < len(prospects):
                wait_time = random.randint(30, 60)
                print(f"\n⏱️  Waiting {wait_time}s before next message...\n")
                time.sleep(wait_time)
        
        print(f"\n{'='*60}")
        print(f"📊 FINAL RESULTS:")
        print(f"   ✅ Successful: {successful}")
        print(f"   ❌ Failed: {failed}")
        print(f"   📈 Success Rate: {(successful/(successful+failed)*100):.1f}%")
        print(f"{'='*60}\n")
        
        return {
            "successful": successful,
            "failed": failed,
            "results": results
        }
        
    finally:
        # Don't close driver automatically - let user see what happened
        print("Browser will stay open. Close it manually when done.")
        # driver.quit()


def test_single_dm_selenium():
    """Test sending a single DM"""
    driver = setup_driver()
    
    try:
        test_prospect = {
            "name": "Dharani",
            "profile_url": "https://www.linkedin.com/in/dharani-k-58609a37a",
            "message": "Hi! This is a test message from my automation system."
        }
        
        result = send_linkedin_dm_selenium(
            driver,
            test_prospect["profile_url"],
            test_prospect["message"]
        )
        
        print(f"\n🧪 Test Result: {result}")
        return result
        
    finally:
        print("\n✅ Test complete! Check the browser to verify.")
        print("Browser will stay open. Close it manually when done.")
        # driver.quit()


if __name__ == "__main__":
    print("="*60)
    print("FREE LinkedIn DM Sender (Selenium)")
    print("="*60)
    print("\n⚠️  PREREQUISITES:")
    print("1. Chrome browser installed")
    print("2. Logged into LinkedIn in Chrome")
    print("3. Install selenium: pip install selenium")
    print("4. Download ChromeDriver: https://chromedriver.chromium.org/")
    print("\n" + "="*60 + "\n")
    
    # Run test
    test_single_dm_selenium()