"""Script to generate training data."""
import logging
from urllib.request import urlopen
import pandas as pd
from bs4 import BeautifulSoup as soup


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s"
)


def get_cricinfo_html_document(page_number: int):
    """Return Cricinfo HTML document of all cricket players."""
    site = f"https://stats.espncricinfo.com/ci/engine/stats/index.html?class=3;home_or_away=1;home_or_away=2;home_or_away=3;result=1;result=2;result=3;result=5;spanmax1=31+Dec+2012;spanval1=span;template=results;type=batting;page={page_number}"
    logging.info(f"Calling URl = {site}")
    # Open that site
    with urlopen(site) as site_object:
        # read data from site
        html_document = site_object.read()
        # # close the object
        site_object.close()
    # scrapping data from site
    sp_page = soup(html_document, "html.parser")
    logging.info("HTML document read")
    return sp_page


def get_cricket_stats_table_element(sp_page):
    """Parse HTML document and return the element which contains cricket stats of players."""
    logging.info("Parsing the HTML document read.")
    stat_parent_div = sp_page.find_all("div", {"class": "pnl650M"})

    parent_container = sp_page.find("div", {"id": "ciHomeContentlhs"})

    stat_parent_div = parent_container.find_all("div", {"class": "pnl650M"})

    player_stat_table = stat_parent_div[0].find_all("table", {"class", "engineTable"})

    engine_table_tag = None
    for each_tag in player_stat_table:
        if "style" not in each_tag.attrs:
            engine_table_tag = each_tag

    logging.info("Found the stats table element.")
    return engine_table_tag


def get_header_list(engine_table_tag):
    """Return Header list cricket stats element table."""
    headers = engine_table_tag.find("thead").find_all("th")

    header_name_list = []
    for each_header in headers:
        child_tag = each_header.find("a")
        if child_tag:
            header_name_list.append(child_tag.contents[0])
        elif each_header.contents:
            header_name_list.append(each_header.contents[0])

    logging.info(f"Header List created = {header_name_list}")
    return header_name_list


def get_player_stats_data(engine_table_tag):
    """Return player stats data from the table element."""
    logging.info("Getting the Player stats data.")
    data = engine_table_tag.find("tbody").find_all("tr", {"class": "data1"})

    batsmen_data = []
    for each_row in data:
        table_data = each_row.findChildren("td")
        row_data = []
        for each_data in table_data:
            has_child_element = len(each_data.find_all()) != 0
            if has_child_element:
                child_element = each_data.find("a")
                if child_element and not child_element.find("img"):
                    row_data.append(child_element.contents[0])
                else:
                    grandson_element = each_data.find("b")
                    if grandson_element:
                        row_data.append(grandson_element.contents[0])
            else:
                row_data.append(each_data.contents[0])
        batsmen_data.append(row_data)

    logging.info(f"Player Stats list data created = {len(batsmen_data)}")
    return batsmen_data


def main():
    """Main method to get all data and combine to form pandas dataframe."""
    input_data_frame = pd.DataFrame()
    for page_number in range(1, 15):
        sp_page = get_cricinfo_html_document(page_number)
        engine_table_tag = get_cricket_stats_table_element(sp_page)
        header_list = get_header_list(engine_table_tag)
        player_stats_data = get_player_stats_data(engine_table_tag)
        player_df = pd.DataFrame(player_stats_data, columns=header_list)
        input_data_frame = pd.concat([input_data_frame, player_df])

    input_data_frame.to_csv("data/international_player_stats_data.csv", index=False)


if __name__ == "__main__":
    main()
