import sqlite3
import pandas as pd
import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
import json
import argparse

# database structure
def create_database(db_name):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cust_name Text,
            cust_sname Text,
            cust_address Text,
            cust_contact Text
        )
    ''')

    conn.commit()
    conn.close()

if __name__ == "__main__":
    create_database('cust_data.db')


def extract_csv(file_path):
    df = pd.read_csv(file_path)
    return df

def scrape_html(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    data = []
    
    # texts structure 
    for paragraph in soup.find_all('p'):
        data.append(paragraph.text)
        
    return data

def parse_xml(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()
    data = []
    
    #  specific XML structure
    for element in root.findall('.//YourElement'):
        data.append(element.text)
        
    return data

def read_json(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data

def insert_data(db_name, cust_name,cust_sname,cust_address,cust_contact):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT INTO data (cust_name,cust_sname,cust_address,cust_contact) VALUES (?,?,?,?)
    ''', (cust_name,cust_sname,cust_address,cust_contact))
    
    conn.commit()
    conn.close()

def extract_and_insert_csv(file_path, db_name):
    df = extract_csv(file_path)
    for _, row in df.iterrows():
        insert_data(db_name, 'CSV', str(row.to_dict()))

def search_data(db_name, query):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT * FROM cust_data WHERE content LIKE ?
    ''', ('%' + query + '%',))
    
    results = cursor.fetchall()
    conn.close()
    return results

def main():
    parser = argparse.ArgumentParser(description='Data Aggregation Tool')
    
    parser.add_argument('--csv', type=str, help='Path to CSV file')
    parser.add_argument('--html', type=str, help='URL to scrape HTML')
    parser.add_argument('--xml', type=str, help='Path to XML file')
    parser.add_argument('--json', type=str, help='Path to JSON file')
    parser.add_argument('--db', type=str, default='cust_data.db', help='SQLite database name')
    parser.add_argument('--search', type=str, help='Query to search in the database')

    args = parser.parse_args()

    
    if args.csv:
        extract_and_insert_csv(args.csv, args.db)
    
    if args.html:
        data = scrape_html(args.html)
        for content in data:
            insert_data(args.db, 'HTML', content)
    
    if args.xml:
        data = parse_xml(args.xml)
        for content in data:
            insert_data(args.db, 'XML', content)
    
    if args.json:
        data = read_json(args.json)
        for content in data:
            insert_data(args.db, 'JSON', str(content))
    
    if args.search:
        results = search_data(args.db, args.search)
        for result in results:
            print(result)

if __name__ == "__main__":
    main()
