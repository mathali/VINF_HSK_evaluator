from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException


options = webdriver.ChromeOptions()
options.add_argument("headless")
driver = webdriver.Chrome("../../chromedriver")#, options=options)

output_file = '../../output/crawler/hskreading.csv'

with open(output_file, 'w', newline='\n', encoding='utf-8') as f:
    f.write('id\tHSK_level\tURL\tTitle_EN\tTitle_ZH\tDescription\tcontent\n')


for category in ['https://hskreading.com/beginner/', 'https://hskreading.com/intermediate/', 'https://hskreading.com/advanced/']:
    driver.get(category)

    while True:
        for article in driver.find_elements_by_tag_name('article'):
            WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.TAG_NAME, "article")))
            level = article.find_element_by_xpath(".//*[@rel='category tag']").text.split(' ')[1]
            title_en = article.find_element_by_tag_name("a").text
            url = article.find_element_by_tag_name("a").get_attribute('href')

            driver.execute_script("window.open('');")
            driver.switch_to.window(driver.window_handles[1])
            driver.get(url)

            article_id = WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.TAG_NAME, "article"))).get_attribute("id").split("-")[1]
            post = driver.find_element_by_class_name("post-content")
            heading = post.find_element_by_tag_name("h2")
            title_zh = heading.text.replace('\n', '')
            children = post.find_elements_by_xpath("./*")
            description = children[0].text.replace('\n', '')
            content = ""
            for child in children[1:]:
                if child.tag_name == 'p':
                    content += child.text.replace('\n', '')
                elif child.tag_name == 'input':
                    break

            with open(output_file, 'a', newline='\n', encoding='utf-8') as f:
                f.write(f'{article_id}\t{level}\t{url}\t{title_en}\t{title_zh}\t{description}\t{content}\n')

            driver.close()
            driver.switch_to.window(driver.window_handles[0])

        try:
            driver.find_element_by_xpath("//*[@rel='next']").click()
        except NoSuchElementException:
            break

driver.close()





