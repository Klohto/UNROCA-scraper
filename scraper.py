# -*- coding: utf-8 -*-
import scrapy
import json
from bs4 import BeautifulSoup
from scrapy.crawler import CrawlerProcess, CrawlerRunner
from twisted.internet import reactor, defer
from scrapy.utils.log import configure_logging


# Category(I-VII)	Final importer state	Number of Items	State of origin (if not exporter)	Intermediate location(s) (if any)	Description of Items	Comments on the transfer
# Category	Final Importer state	Number of Items	State of origin (if not exporter)	Intermediate location(s) (if any)	Description of Items	Comments on the transfer
class ArmsTransferItem(scrapy.Item):
    table = scrapy.Field()
    year = scrapy.Field()
    exporter = scrapy.Field()
    importer = scrapy.Field()
    category = scrapy.Field()
    num_items = scrapy.Field()
    state_of_origin = scrapy.Field()  # Only has a value if state_of_origin != exporter
    intermediate_locations = scrapy.Field()  # Only has a value if filled
    description = scrapy.Field()
    comments = scrapy.Field()

# Category(I-VII)	Number of Items	    Description of Items	    Comments on the transfer
class HoldingsItem(scrapy.Item):
    table = scrapy.Field()
    year = scrapy.Field()
    country = scrapy.Field()
    category = scrapy.Field()
    num_items = scrapy.Field()
    description = scrapy.Field()
    comments = scrapy.Field()

class YearsItem(scrapy.Item):
    years = scrapy.Field()
    country = scrapy.Field()

class CountryItem(scrapy.Item):
    country = scrapy.Field()

class CountriesSpider(scrapy.Spider):
    name = "countries"
    allowed_domains = ['unroca.org']

    custom_settings = {
            "FEEDS": {
                "countries.json": {
                    "format": "json",
                    "overwrite": True
                },
            },
    }

    def start_requests(self):
        start_urls = ["https://www.unroca.org/api/country-list"]
        for url in start_urls:
            yield scrapy.Request(url, self.parse)

    def parse(self, response):
        # load the JSON data from the response
        data = response.json()
        for country in data:
            item = CountryItem()
            item["country"] = country["countryname_slug"]
            yield item

# Scrape the available report years from list of countries
class YearsSpider(scrapy.Spider):
    name = "years"
    allowed_domains = ['unroca.org']

    custom_settings = {
            "FEEDS": {
                "years.json": {
                    "format": "json",
                    "overwrite": True
                },
            },
    }

    @staticmethod
    def get_country_names():
        with open('countries.json') as f:
            country_list = json.load(f)
        for item in country_list:
            yield item["country"]

    def start_requests(self):
        country_names = set(YearsSpider.get_country_names())
        start_urls = [f"https://www.unroca.org/api/{country}" for country in country_names]
        for url in start_urls:
            yield scrapy.Request(url, self.parse)

    def parse(self, response):
        # load the JSON data from the response
        data = response.json()
        available_reports = YearsItem()
        available_reports["country"] = data["country"]["countryname_slug"]
        available_reports["years"] = data.get("available_reports", [])
        yield available_reports

# Export & Import
def parse_arms_transfer_row(rows, table_id, row_year, row_state, is_export = True):
    for row in rows:
        item = ArmsTransferItem()

        row_soup = BeautifulSoup(row.extract(), 'html.parser')
        row_data = row_soup.find_all('td')

        item['table'] = table_id
        item['year'] = row_year
        item['exporter'] = row_state if is_export else row_data[0].text.strip()
        item['importer'] = row_data[0].text.strip() if is_export else row_state
        item['category'] = row_soup.find('th').text.strip()
        item['num_items'] = row_data[1].text.strip()
        item['state_of_origin'] = row_data[2].text.strip()
        item['intermediate_locations'] = row_data[3].text.strip()
        item['description'] = row_data[4].text.strip()
        item['comments'] = row_data[5].text.strip()

        yield item

# Military & Production
def parse_holdings_row(rows, table_id, row_year, row_state):
    for row in rows:
        item = HoldingsItem()

        row_soup = BeautifulSoup(row.extract(), 'html.parser')
        row_data = row_soup.find_all('td')

        item['table'] = table_id
        item['year'] = row_year
        item['country'] = row_state
        item['category'] = row_soup.find('th').text.strip()
        item['num_items'] = row_data[0].text.strip()
        item['description'] = row_data[1].text.strip()
        item['comments'] = row_data[2].text.strip()

        yield item

class UnrocaSpider(scrapy.Spider):
    # countryname_slug
    name = 'unroca'
    allowed_domains = ['unroca.org']
    
    custom_settings = {
            "FEEDS": {
                "unroca.json": {
                    "format": "json",
                    "overwrite": True
                },
            },
    }

    @staticmethod
    def get_country_names():
        with open('countries.json') as f:
            country_list = json.load(f)
        for item in country_list:
            yield item["country"]

    @staticmethod
    def get_country_years():
        with open('years.json') as f:
            country_list = json.load(f)
        for item in country_list:
            yield item

    def start_requests(self):
        country_names = set(UnrocaSpider.get_country_names())
        years = UnrocaSpider.get_country_years()
        country_years = dict()
        for year in years:
            country_years[year["country"]] = year["years"]

        start_urls = []

        for country in country_names:
            for year in country_years[country]:
                report_year = year["year"]
                start_urls.append(f"https://www.unroca.org/{country}/report/{report_year}/")

        for url in start_urls:
            yield scrapy.Request(url, self.parse)

    def parse(self, response):
        if response.status == 200:

            # Due to random UN redirects ensure that we are looking at a state's original report.
            doc_h4 = response.selector.xpath('//h4[contains(@class, "unroca")]')
            doc_h4_text = doc_h4.xpath('./text()')
            if len(doc_h4_text) >= 1 and doc_h4_text[0].extract() == 'UNROCA original report':
                report_details = doc_h4.xpath('following-sibling::*/text()')[0].extract().split()
                reporting_year = report_details[-1]
                reporting_state = ' '.join(report_details[:-1])
                div_panels = response.selector.xpath('//div[contains(@class, "panel-body")]')
                export_trows = div_panels[1].xpath('./table/tbody/tr')
                import_trows = div_panels[2].xpath('./table/tbody/tr')
                military_holdings_trows = div_panels[3].xpath('./table/tbody/tr')
                national_productions_trows = div_panels[4].xpath('./table/tbody/tr')
                _policies_trows = div_panels[5].xpath('./table/tbody/tr') # ignored
                small_arms_exports_trows = div_panels[6].xpath('./table/tbody/tr') if len(div_panels) > 9 else [] # optional before 2006
                light_weapons_exports_trows = div_panels[7].xpath('./table/tbody/tr') if len(div_panels) > 9 else [] # optional before 2006
                small_arms_imports_trows = div_panels[8].xpath('./table/tbody/tr') if len(div_panels) > 9 else [] # optional before 2006
                light_weapons_imports_trows = div_panels[9].xpath('./table/tbody/tr') if len(div_panels) > 9 else [] # optional before 2006

                yield from parse_arms_transfer_row(export_trows, 'major_export', is_export = True, row_year = reporting_year, row_state = reporting_state)
                yield from parse_arms_transfer_row(import_trows, 'major_import', is_export = False, row_year = reporting_year, row_state = reporting_state)
                yield from parse_holdings_row(military_holdings_trows, 'military_holdings', row_year = reporting_year, row_state = reporting_state)
                yield from parse_holdings_row(national_productions_trows, 'national_production', row_year = reporting_year, row_state = reporting_state)
                yield from parse_arms_transfer_row(small_arms_exports_trows, 'small_arms_export', is_export = True, row_year = reporting_year, row_state = reporting_state)
                yield from parse_arms_transfer_row(small_arms_imports_trows, 'small_arms_import', is_export = False, row_year = reporting_year, row_state = reporting_state)
                yield from parse_arms_transfer_row(light_weapons_exports_trows, 'light_weapons_export', is_export = True, row_year = reporting_year, row_state = reporting_state)
                yield from parse_arms_transfer_row(light_weapons_imports_trows, 'light_weapons_import', is_export = False, row_year = reporting_year, row_state = reporting_state)

# Bunch of magic to run it in sequence

@defer.inlineCallbacks
def crawl():
    # uncomment if you want to refetch countries and years
    # yield runner.crawl(CountriesSpider)
    # yield runner.crawl(YearsSpider)
    yield runner.crawl(UnrocaSpider)
    reactor.stop()

# First scrape the countries,
# Then the available years,
# Then run the report scraper
if __name__ == "__main__":
    configure_logging({"LOG_FORMAT": "%(levelname)s: %(message)s"})
    runner = CrawlerRunner()

    crawl()
    reactor.run() # the script will block here until the last crawl call is finished