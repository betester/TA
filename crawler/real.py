from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options

firefox_options = Options()
firefox_options.add_argument("--headless")

def login(username, password):

    # Read thread file and open the link
    driver.get("https://twitter.com/i/flow/login")

    # Login
    driver.find_element(By.CSS_SELECTOR, "input[autocomplete='username']").send_keys(username)
    for button in driver.find_elements(By.CSS_SELECTOR, "div[role='button']"):
        if button.text == "Next": button.click(); break # The location for the next button varied. We have to use for loop.
    driver.find_element(By.CSS_SELECTOR, "input[autocomplete='current-password']").send_keys(password)
    driver.find_element(By.CSS_SELECTOR, "div[data-testid='LoginForm_Login_Button']").click()


login("Ikrammullah02", "")


service = webdriver.FirefoxService(executable_path="./geckodriver", options=firefox_options) 
driver = webdriver.Firefox(service=service)

driver.get("file:///home/betester/Kuliah/TA/crawler/kebakaran%20-%20Pencarian%20_%20X.html")

contents = driver.find_elements(By.TAG_NAME, "article")

for content in contents:
    print(content.text)

