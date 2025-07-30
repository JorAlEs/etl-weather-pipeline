from selenium import webdriver
from selenium.webdriver.chrome.options import Options

options = Options()
options.add_argument("--headless")  # Opcional si no quieres abrir ventana
driver = webdriver.Chrome(options=options)
driver.get("https://google.com")
print(driver.title)
driver.quit()

