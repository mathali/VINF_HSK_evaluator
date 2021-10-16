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

# Browse all articles on the hskreading website. Beginner - HSK1 2, intermediate - HSK3 4, advanced - HSK5 6
for category in ['https://hskreading.com/beginner/', 'https://hskreading.com/intermediate/', 'https://hskreading.com/advanced/']:
    driver.get(category)    # Load page

    # Iterate until fully explored
    while True:
        # Grab each article element on page
        for article in driver.find_elements_by_tag_name('article'):

            # Make sure page is loaded
            WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.TAG_NAME, "article")))

            # Extract metadata about article
            level = article.find_element_by_xpath(".//*[@rel='category tag']").text.split(' ')[1]
            title_en = article.find_element_by_tag_name("a").text
            url = article.find_element_by_tag_name("a").get_attribute('href')

            # Open article in new winow to access content
            driver.execute_script("window.open('');")
            driver.switch_to.window(driver.window_handles[1])
            driver.get(url)

            # Make sure new page is loaded, grab article id for later reference
            article_id = WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.TAG_NAME, "article"))).get_attribute("id").split("-")[1]

            # Extract Title and Content from the unique structure of the website
            post = driver.find_element_by_class_name("post-content")
            heading = post.find_element_by_tag_name("h2")
            title_zh = heading.text.replace('\n', '')
            children = post.find_elements_by_xpath("./*")
            description = children[0].text.replace('\n', '')
            content = ""
            for child in children[1:]:
                if child.tag_name == 'p':
                    content += child.text.replace('\n', '')     # Clean article so it can be stored in csv
                elif child.tag_name == 'input':
                    break

            # Append Metadata + Article to csv file
            with open(output_file, 'a', newline='\n', encoding='utf-8') as f:
                f.write(f'{article_id}\t{level}\t{url}\t{title_en}\t{title_zh}\t{description}\t{content}\n')

            # Close window with article contents and find another in list
            driver.close()
            driver.switch_to.window(driver.window_handles[0])

        # Explore all pages of articles in category
        try:
            driver.find_element_by_xpath("//*[@rel='next']").click()
        except NoSuchElementException:
            break

driver.close()
