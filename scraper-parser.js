const puppeteer = require("puppeteer");
const createCsvWriter = require("csv-writer").createObjectCsvWriter;
const csvParse = require("csv-parse");
const fs = require("fs");

const API_KEY = JSON.parse(fs.readFileSync("config.json")).api_key;

async function writeToCsv(data, outputFile) {
    if (!data || data.length === 0) {
        throw new Error("No data to write!");
    }
    const fileExists = fs.existsSync(outputFile);

    const headers = Object.keys(data[0]).map(key => ({id: key, title: key}))

    const csvWriter = createCsvWriter({
        path: outputFile,
        header: headers,
        append: fileExists
    });
    try {
        await csvWriter.writeRecords(data);
    } catch (e) {
        throw new Error("Failed to write to csv");
    }
}

async function readCsv(inputFile) {
    const results = [];
    const parser = fs.createReadStream(inputFile).pipe(csvParse.parse({
        columns: true,
        delimiter: ",",
        trim: true,
        skip_empty_lines: true
    }));

    for await (const record of parser) {
        results.push(record);
    }
    return results;
}

function range(start, end) {
    const array = [];
    for (let i=start; i<end; i++) {
        array.push(i);
    }
    return array;
}

function getScrapeOpsUrl(url, location="us") {
    const params = new URLSearchParams({
        api_key: API_KEY,
        url: url,
        country: location
    });
    return `https://proxy.scrapeops.io/v1/?${params.toString()}`;
}

async function scrapeSearchResults(browser, keyword, pageNumber, location="us", retries=3) {
    let tries = 0;
    let success = false;

    while (tries <= retries && !success) {
        
        const formattedKeyword = keyword.replace(" ", "+");
        const page = await browser.newPage();
        try {
            const url = `https://www.g2.com/search?page=${pageNumber+1}&query=${formattedKeyword}`;
    
            const proxyUrl = getScrapeOpsUrl(url, location);
            await page.goto(proxyUrl);

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

                await writeToCsv([businessInfo], `${keyword.replace(" ", "-")}.csv`);
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

async function startScrape(keyword, pages, location, concurrencyLimit, retries) {
    const pageList = range(0, pages);

    const browser = await puppeteer.launch()

    while (pageList.length > 0) {
        const currentBatch = pageList.splice(0, concurrencyLimit);
        const tasks = currentBatch.map(page => scrapeSearchResults(browser, keyword, page, location, retries));

        try {
            await Promise.all(tasks);
        } catch (err) {
            console.log(`Failed to process batch: ${err}`);
        }
    }

    await browser.close();
}

async function processBusiness(browser, row, location, retries = 3) {
    const url = row.g2_url;
    let tries = 0;
    let success = false;

    
    while (tries <= retries && !success) {
        const page = await browser.newPage();

        try {
            await page.goto(url);

            const reviewCards = await page.$$("div[class='paper paper--white paper--box mb-2 position-relative border-bottom']");
            let anonCount = 0;

            for (const reviewCard of reviewCards) {
                reviewDateElement = await reviewCard.$("time");
                reviewTextElement = await reviewCard.$("div[itemprop='reviewBody']");

                if (reviewDateElement && reviewTextElement) {
                    const date = await page.evaluate(element => element.getAttribute("datetime"), reviewDateElement);
                    const reviewBody = await page.evaluate(element => element.textContent, reviewTextElement);

                    const nameElement = await reviewCard.$("a[class='link--header-color']");
                    let name;
                    if (nameElement) {
                        name = await page.evaluate(element => element.textContent, nameElement);
                    } else {
                        name = `anonymous-${anonCount}`;
                        anonCount++;
                    }

                    const jobTitleElement = await reviewCard.$("div[class='mt-4th']");
                    let jobTitle;
                    if (jobTitleElement) {
                        jobTitle = await page.evaluate(element => element.textContent, jobTitleElement);
                    } else {
                        jobTitle = "n/a";
                    }

                    const ratingContainer = await reviewCard.$("div[class='f-1 d-f ai-c mb-half-small-only']");
                    const ratingDiv = await ratingContainer.$("div");
                    const ratingClass = await page.evaluate(element => element.getAttribute("class"), ratingDiv);
                    const ratingArray = ratingClass.split("-");
                    const rating = Number(ratingArray[ratingArray.length-1])/2;

                    const infoContainer = await reviewCard.$("div[class='tags--teal']");
                    const incentivesDirty = await infoContainer.$$("div");
                    const incentivesClean = [];

                    let source = "";
                    for (const incentive of incentivesDirty) {
                        const text = await page.evaluate(element => element.textContent, incentive);
                        if (!incentivesClean.includes(text)) {
                            if (text.includes("Review source:")) {
                                textArray = text.split(": ");
                                source = textArray[textArray.length-1];
                            } else {
                                incentivesClean.push(text);
                            }
                        }
                    }
                    const validated = incentivesClean.includes("Validated Reviewer");
                    const incentivized = incentivesClean.includes("Incentivized Review");

                    const reviewData = {
                        name: name,
                        date: date,
                        job_title: jobTitle,
                        rating: rating,
                        full_review: reviewBody,
                        review_source: source,
                        validated: validated,
                        incentivized: incentivized
                    }
                    console.log(reviewData);
                }
            }

            success = true;
        } catch (err) {
            console.log(`Error: ${err}, tries left: ${retries-tries}`);
            tries++;
        } finally {
            await page.close();
        }
    } 
}

async function processResults(csvFile, location, retries) {
    const businesses = await readCsv(csvFile);
    const browser = await puppeteer.launch();

    for (const business of businesses) {
        await processBusiness(browser, business, location, retries);
    }
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
        await startScrape(keyword, pages, location, concurrencyLimit, retries);
        console.log("Crawl complete");
        aggregateFiles.push(`${keyword.replace(" ", "-")}.csv`);
    }

    for (const file of aggregateFiles) {
        await processResults(file, location, concurrencyLimit, retries);
    }
}


main();