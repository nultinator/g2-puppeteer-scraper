const puppeteer = require("puppeteer");
const createCsvWriter = require("csv-writer").createObjectCsvWriter;
const csvParse = require("csv-parse");
const fs = require("fs");

const API_KEY = JSON.parse(fs.readFileSync("config.json")).api_key;

function range(start, end) {
    const array = [];
    for (let i=start; i<end; i++) {
        array.push(i);
    }
    return array;
}

async function scrapeSearchResults(browser, keyword, location="us", retries=3) {
    let tries = 0;
    let success = false;

    while (tries <= retries && !success) {
        
        const formattedKeyword = keyword.replace(" ", "+");
        const page = await browser.newPage();
        try {
            const url = `https://www.g2.com/search?query=${formattedKeyword}`;
    
            await page.goto(url);

            console.log(`Successfully fetched: ${url}`);

            const divCards = await page.$$("div[class='product-listing mb-1 border-bottom']");

            for (const divCard of divCards) {

                const nameElement = await divCard.$("div[class='product-listing__product-name']");
                const name = await page.evaluate(element => element.textContent, nameElement);

                const g2UrlElement = await nameElement.$("a");
                const g2Url = await page.evaluate(element => element.getAttribute("href"), g2UrlElement);

                let rating = 0.0;
                const ratingElement = await divCard.$("span[class='fw-semibold']");
                if (ratingElement) {
                    rating = await page.evaluate(element => element.textContent, ratingElement);
                }

                const descriptionElement = await divCard.$("p");
                const description = await page.evaluate(element => element.textContent, descriptionElement)

                const businessInfo = {
                    name: name,
                    stars: rating,
                    g2_url: g2Url,
                    description: description
                };

                console.log(businessInfo);
            }


            success = true;
        } catch (err) {
            console.log(`Error: ${err}, tries left ${retries - tries}`);
            tries++;
        } finally {
            await page.close();
        } 
    }
}

async function startScrape(keyword, location, retries) {

    const browser = await puppeteer.launch()

    await scrapeSearchResults(browser, keyword, location, retries)
    await browser.close();
}

async function main() {
    const keywords = ["online bank"];
    const concurrencyLimit = 5;
    const pages = 1;
    const location = "us";
    const retries = 3;
    const aggregateFiles = [];

    for (const keyword of keywords) {
        console.log("Crawl starting");
        await startScrape(keyword, location, retries);
        console.log("Crawl complete");
        aggregateFiles.push(`${keyword.replace(" ", "-")}.csv`);
    }
}


main();