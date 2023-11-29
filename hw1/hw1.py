import json
import re
import time
import csv
from bs4 import BeautifulSoup
import requests
from random import randint
from html.parser import HTMLParser
from urllib.parse import urlparse, unquote

USER_AGENT = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/61.0.3163.100 Safari/537.36'}


class SearchEngine:
    @staticmethod
    def search(query, sleep=True):
        search_results = []
        for page in range(1, 10):
            if sleep:  # Prevents loading too many pages too soon
                time.sleep(randint(2, 5))
            temp_url = '+'.join(query.split())
            url = ' https://search.yahoo.com/search?p=' + temp_url + '&b=' + str(page)
            soup = BeautifulSoup(requests.get(
                url, headers=USER_AGENT).text, "html.parser")
            new_results = SearchEngine.scrape_search_result(soup)
            search_results.extend(new_results)

            if len(search_results) >= 10:
                break

        return search_results[:10]

    @staticmethod
    def scrape_search_result(soup):
        raw_results = soup.find_all("a", attrs={"class": "d-ib fz-20 lh-26 td-hu tc va-bot mxw-100p"})
        results = []

        if len(raw_results) < 10:
            threshold = len(raw_results)
        else:
            threshold = 10

        for result in raw_results:
            link = result.get('href')
            cleaned_link = SearchEngine.process_link(link)
            if cleaned_link not in results:
                results.append(cleaned_link)
            if len(results) >= threshold:
                break

        return results

    @staticmethod
    def process_link(link):
        match = re.search(r"RU=(.*?)\/", link)
        if match:
            cleaned_link = match.group(1)
            cleaned_link = unquote(cleaned_link)
            return cleaned_link

    # @staticmethod
    # def main(): # test script main method
    #     # create a search engine
    #     search_engine = SearchEngine()
    #
    #     # searching
    #     query = input("Enter your search query: ")
    #     search_results = search_engine.search(query)
    #
    #     # print search results
    #     print("Search Results for '{}':".format(query))
    #     for i, result in enumerate(search_results, start=1):
    #         print("{}. {}".format(i, result))


# the function for reading queries in the set
def read_queries(filename):
    with open(filename) as f:
        lines = f.read().splitlines()
    return lines


# the function for find the matched results in two different SE query sets
def find_matches(yahoo, google):
    matches = []
    for query in yahoo:
        temp = []
        for url in range(len(yahoo[query])):
            if yahoo[query][url] in google[query]:
                temp.append([google[query].index(yahoo[query][url]) + 1, url + 1])
        matches.append(temp)
    return matches


# the function for calculating spearman coefficient
def spearman_coefficient(data):
    overlap_list = []
    overlap_percent_list = []
    spearman_coefficient_list = []
    sum_overlap = 0
    sum_overlap_percent = 0
    sum_spearman_coefficient = 0

    for matches in data:
        n = len(matches)
        overlap_list.append(n)
        sum_overlap += n

        percent = round(len(matches) / 10 * 100.0, 1)
        overlap_percent_list.append(percent)
        sum_overlap_percent += percent

        d_2s = []
        if n == 0:
            spearman_coefficient_list.append(0)
        else:
            for match in matches:
                d_2 = (match[0] - match[1]) ** 2
                d_2s.append(d_2)
            # If n = 1(which means only one paired match), we deal with it in a different way:
            # 1. if Rank in your result = Rank in Google result → rho = 1
            # 2. if Rank in your result ≠ Rank in Google result → rho = 0
            if n == 1:
                if match[0] == match[1]:
                    spearman_coefficient_list.append(1)
                    sum_spearman_coefficient += 1
                else:
                    spearman_coefficient_list.append(0)
            else:
                spearman_coefficient = 1 - 6 * sum(d_2s) / (n * (n ** 2 - 1))
                sum_spearman_coefficient += spearman_coefficient
                spearman_coefficient_list.append(spearman_coefficient)

    # Calculate averages
    avg_overlap = sum_overlap / 100
    avg_overlap_percent = sum_overlap_percent / 100
    avg_spearman_coefficient = sum_spearman_coefficient / 100
    return overlap_list, overlap_percent_list, spearman_coefficient_list, avg_overlap, avg_overlap_percent, avg_spearman_coefficient


if __name__ == '__main__':
    # SearchEngine.main()
    query_file = "./100QueriesSet2.txt"
    Google_file = "./Google_Result2.json"
    hw1_json = "./hw1.json"
    hw1_csv = "./hw1.csv"

    yahoo = SearchEngine()
    results = {}

    # Get Yahoo URL results
    queries = read_queries(query_file)
    for query in queries:
        result = query.rstrip()
        results[result] = yahoo.search(query)
    out_json = json.dumps(results, indent=2)
    with open(hw1_json, 'w') as f:
        f.write(out_json)

    # Get matches between Google and Yahoo
    yahoo_result = json.load(open(hw1_json))
    google_result = json.load(open(Google_file))
    overlap = find_matches(yahoo_result, google_result)

    # Spearman Coefficient
    overlap_list, overlap_percent_list, spearman_coefficient_list, avg_overlap, avg_overlap_percent, avg_spearman_coefficient = spearman_coefficient(
        overlap)

    result_str = 'Queries, Number of Overlapping Results, Percent Overlap, Spearman Coefficient\n'
    for i in range(len(overlap_list)):
        temp_str = 'Query ' + str(i + 1) + ', ' + str(overlap_list[i]) + ', ' + str(
            overlap_percent_list[i]) + ', ' + str(spearman_coefficient_list[i]) + '\n'
        result_str += temp_str
    temp_str = 'Averages, ' + str(avg_overlap) + ', ' + str(avg_overlap_percent) + ', ' + str(avg_spearman_coefficient)
    result_str += temp_str
    print(result_str)
    with open(hw1_csv, 'w') as f:
        f.write(result_str)
