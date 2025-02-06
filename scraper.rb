require 'nokogiri'
require 'open-uri'
require 'sqlite3'
require 'logger'
require 'date'

# Initialize the logger
logger = Logger.new(STDOUT)

# Define the URL of the page
url = 'https://tasman.tas.gov.au/advertised-applications/'

# Step 1: Fetch the page content
begin
  logger.info("Fetching page content from: #{url}")
  page_html = open(url).read
  logger.info("Successfully fetched page content.")
rescue => e
  logger.error("Failed to fetch page content: #{e}")
  exit
end

# Step 2: Parse the page content using Nokogiri
doc = Nokogiri::HTML(page_html)

# Step 3: Initialize the SQLite database
db = SQLite3::Database.new "data.sqlite"

# Create table
db.execute <<-SQL
  CREATE TABLE IF NOT EXISTS tasman (
    id INTEGER PRIMARY KEY,
    description TEXT,
    date_scraped TEXT,
    date_received TEXT,
    on_notice_to TEXT,
    address TEXT,
    council_reference TEXT,
    applicant TEXT,
    owner TEXT,
    stage_description TEXT,
    stage_status TEXT,
    document_description TEXT,
    title_reference TEXT
  );
SQL

# Define variables for storing extracted data for each entry
address = ''  
description = ''
on_notice_to = ''
title_reference = ''
date_received = ''
council_reference = ''
applicant = ''
owner = ''
stage_description = ''
stage_status = ''
document_description = ''
date_scraped = Date.today.to_s

# Step 4: Extract data for each document
doc.css('.wpfilebase-file-default').each_with_index do |row, index|
  description = row.at_css('.filetitle a').text.strip
  council_reference = description.split(' - ').first
  document_description = row.at_css('.filetitle a')['href']
  on_notice_to = description.match(/(\d{1,2} [A-Za-z]+ \d{4})/)&.captures&.first
  date_received = row.at_css('.details tr td:contains("Date:")').next_element.text.strip
  
  # Log the extracted data for debugging purposes
  logger.info("Extracted Data: Title: #{description}, Date Received: #{date_received}, URL: #{document_description}, Council Reference: #{council_reference}, On Notice To: #{on_notice_to}")

  # Step 5: Ensure the entry does not already exist before inserting
  existing_entry = db.execute("SELECT * FROM tasman WHERE council_reference = ?", council_reference)

  if existing_entry.empty? # Only insert if the entry doesn't already exist
    # Save data to the database
    db.execute("INSERT INTO tasman (description, date_received, document_description, council_reference, on_notice_to) 
      VALUES (?, ?, ?, ?, ?)", [description, date_received, document_description, council_reference, on_notice_to])

    logger.info("Data for #{council_reference} saved to database.")
  else
    logger.info("Duplicate entry for document #{council_reference} found. Skipping insertion.")
  end
end

# Finish
logger.info("Data has been successfully inserted into the database.")
