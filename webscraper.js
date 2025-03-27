const { Pool } = require('pg');
const axios = require('axios');
const cheerio = require('cheerio');
const url = require('url');
require('dotenv').config();
/**
 * Connects to the database and retrieves companies with sites but no email/phone
 * @returns {Promise<Array>} Array of company records
 * 
 */

async function easyNotificationsRequest(message, type, category, channel, content) {
  var requestData = [];

  let config = {
      method: "POST",
      headers: {
         "content-type": "application/json",
      },
      url: 'https://notifications-app-api-production.up.railway.app/send-message',
      data: {
          message: message,
          type: type,
          category: category,
          route: "felipe",
          channel: channel,
          content: content
      },
  };

  try {
      const response = await axios.request(config);
      return response.data;
  } catch (error) {
      console.log(error);
      easyNotificationsRequest("Erro na raspagem em"+process.env.SCRAP_UF, "error", "cadastrobr", "cadastrobr", error)
  }
}


async function getCompaniesForScraping() {
  console.log(`[${new Date().toISOString()}] Connecting to database to retrieve company sites...`);
  
  const pool = new Pool({
    host: 'shortline.proxy.rlwy.net',
    port: 24642,
    database: 'railway',
    user: 'postgres',
    password: 'DSBlkKCnEUKRlGBbnyhofNcwkhwvINsp',
    connectionTimeoutMillis: 15000
  });

  try {
    await pool.query('SELECT 1');
    console.log(`[${new Date().toISOString()}] Connected to database successfully`);
    
    const result = await pool.query(`
      SELECT id, cnpj, site 
      FROM transfer 
      WHERE site IS NOT NULL 
      AND at = '1'
      AND uf = '${process.env.SCRAP_UF}'
    `);
    
    console.log(`[${new Date().toISOString()}] Retrieved ${result.rows.length} companies for scraping`);
    easyNotificationsRequest("Iniciando raspagem de industrias do "+process.env.SCRAP_UF, "info", "cadastrobr", "cadastrobr", +result.rows.length+" industrias")
    return result.rows;
  } catch (error) {
    console.error(`[${new Date().toISOString()}] Database error:`, error.message);
    easyNotificationsRequest("Erro na raspagem em"+process.env.SCRAP_UF, "error", "cadastrobr", "cadastrobr", error.message)
    throw error;
  } finally {
    await pool.end();
  }
}

/**
 * Extracts emails from HTML content
 * @param {string} html - HTML content
 * @returns {string|null} - First email found or null
 */
function extractEmails(html) {
  const emailRegex = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;
  const matches = html.match(emailRegex);
  
  if (matches && matches.length > 0) {
    // Filter out common false positives
    const filteredEmails = matches.filter(email => {
      return !email.includes('exemplo') && 
             !email.includes('example') && 
             !email.includes('user') &&
             !email.includes('email@');
    });
    
    return filteredEmails.length > 0 ? filteredEmails[0] : null;
  }
  
  return null;
}

/**
 * Extracts phone numbers from HTML content
 * @param {string} html - HTML content
 * @returns {object|null} - Object with ddd and number or null
 */
function extractPhoneNumbers(html) {
  // Replace HTML entities and normalize spaces
  const cleanHtml = html.replace(/&nbsp;/g, ' ')
                        .replace(/\s+/g, ' ');
  
  // Various phone number patterns for Brazil
  const phonePatterns = [
    // (XX) XXXX-XXXX or (XX)XXXX-XXXX
    /\((\d{2})\)\s*(\d{4})-(\d{4})/g,
    // (XX) XXXXX-XXXX or (XX)XXXXX-XXXX
    /\((\d{2})\)\s*(\d{5})-(\d{4})/g,
    // XX XXXX-XXXX or XX XXXX XXXX
    /\b(\d{2})\s*(\d{4})[\s-](\d{4})\b/g,
    // XX XXXXX-XXXX or XX XXXXX XXXX
    /\b(\d{2})\s*(\d{5})[\s-](\d{4})\b/g
  ];

  for (const pattern of phonePatterns) {
    const matches = [...cleanHtml.matchAll(pattern)];
    if (matches.length > 0) {
      const match = matches[0];
      const ddd = match[1];
      let number;
      
      if (match[3]) { // If capturing 3 groups (DDD, first part, second part)
        number = match[2] + match[3];
      } else {
        number = match[2];
      }
      
      return { ddd, number };
    }
  }
  
  return null;
}

/**
 * Scrapes a website for contact information
 * @param {string} siteUrl - URL to scrape
 * @returns {Promise<object>} - Object with email and phone data
 */
async function scrapeWebsite(siteUrl) {
  // Make sure the URL has protocol
  if (!siteUrl.startsWith('http')) {
    siteUrl = `https://${siteUrl}`;
  }
  
  console.log(`[${new Date().toISOString()}] Scraping website: ${siteUrl}`);
  
  const result = {
    email: null,
    tel1_dd: null,
    tel1: null
  };
  
  try {
    // Get homepage
    const response = await axios.get(siteUrl, {
      timeout: 10000,
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
      }
    });
    
    const $ = cheerio.load(response.data);
    const html = $.html();
    
    // Extract email and phone from homepage
    result.email = extractEmails(html);
    const phone = extractPhoneNumbers(html);
    if (phone) {
      result.tel1_dd = phone.ddd;
      result.tel1 = phone.number;
    }
    
    // If contact info is still missing, try to find and scrape contact page
    if (!result.email || !result.tel1) {
      const contactLinks = $('a').filter((i, el) => {
        const text = $(el).text().toLowerCase();
        const href = $(el).attr('href') || '';
        return text.includes('contato') || 
               text.includes('contact') || 
               href.includes('contato') || 
               href.includes('contact');
      });
      
      if (contactLinks.length > 0) {
        const contactHref = $(contactLinks[0]).attr('href');
        if (contactHref) {
          const contactUrl = url.resolve(siteUrl, contactHref);
          console.log(`[${new Date().toISOString()}] Found contact page: ${contactUrl}`);
          
          try {
            const contactResponse = await axios.get(contactUrl, {
              timeout: 10000,
              headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
              }
            });
            
            const contactHtml = contactResponse.data;
            
            // Only update if not already found
            if (!result.email) {
              result.email = extractEmails(contactHtml);
            }
            
            if (!result.tel1) {
              const contactPhone = extractPhoneNumbers(contactHtml);
              if (contactPhone) {
                result.tel1_dd = contactPhone.ddd;
                result.tel1 = contactPhone.number;
              }
            }
          } catch (error) {
            console.error(`[${new Date().toISOString()}] Error scraping contact page:`, error.message);
          }
        }
      }
    }
    
    console.log(`[${new Date().toISOString()}] Scraping results for ${siteUrl}:`, {
      email: result.email,
      phone: result.tel1_dd ? `(${result.tel1_dd}) ${result.tel1}` : null
    });
    
    return result;
    
  } catch (error) {
    console.error(`[${new Date().toISOString()}] Error scraping ${siteUrl}:`, error.message);
    return result;
  }
}

/**
 * Updates company data in database
 * @param {number} id - Company ID
 * @param {object} data - Data to update
 * @returns {Promise<boolean>} - Success status
 */
async function updateCompanyData(id, data) {
  const pool = new Pool({
    host: 'shortline.proxy.rlwy.net',
    port: 24642,
    database: 'railway',
    user: 'postgres',
    password: 'DSBlkKCnEUKRlGBbnyhofNcwkhwvINsp',
    connectionTimeoutMillis: 15000
  });
  
  try {
    const updateFields = [];
    const values = [];
    let paramIndex = 1;
    
    // Always set at = 1 to indicate this record has been processed
    updateFields.push(`at = '5'`);
    
    if (data.email) {
      updateFields.push(`email = $${paramIndex}`);
      values.push(data.email);
      paramIndex++;
    }
    
    if (data.tel1) {
      updateFields.push(`tel1 = $${paramIndex}`);
      values.push(data.tel1);
      paramIndex++;
      
      if (data.tel1_dd) {
        updateFields.push(`tel1_dd = $${paramIndex}`);
        values.push(data.tel1_dd);
        paramIndex++;
      }
    }
    
    // No longer need to check if updateFields is empty since we always add at=1
    
    values.push(id);
    
    const query = `
      UPDATE transfer
      SET ${updateFields.join(', ')}
      WHERE id = $${paramIndex}
    `;
    
    await pool.query(query, values);
    console.log(`[${new Date().toISOString()}] Updated company ID ${id} with at=1 and any new contact information`);
    return true;
  } catch (error) {
    console.error(`[${new Date().toISOString()}] Error updating company ID ${id}:`, error.message);
    easyNotificationsRequest("Erro na raspagem  em"+process.env.SCRAP_UF, "error", "cadastrobr", "cadastrobr", error.message)
    return false;
  } finally {
    await pool.end();
  }
}

/**
 * Main function to run the scraper
 */
async function main() {
  console.log(`[${new Date().toISOString()}] ===== STARTING WEB SCRAPER =====`);

  const pool = new Pool({
    host: 'shortline.proxy.rlwy.net',
    port: 24642,
    database: 'railway',
    user: 'postgres',
    password: 'DSBlkKCnEUKRlGBbnyhofNcwkhwvINsp',
    connectionTimeoutMillis: 15000
  });
  
  try {
    const companies = await getCompaniesForScraping();
    
    if (companies.length === 0) {
      console.log(`[${new Date().toISOString()}] No companies found that need scraping.`);
      return;
    }
    
    console.log(`[${new Date().toISOString()}] Beginning to scrape ${companies.length} websites`);
    
    let successCount = 0;
    for (let i = 0; i < companies.length; i++) {
      const company = companies[i];
      console.log(`[${new Date().toISOString()}] Processing ${i+1}/${companies.length}: ${company.site} (CNPJ: ${company.cnpj})`);
      
      try {
        const scrapedData = await scrapeWebsite(company.site);
        
        // Update ALL records, regardless of whether we found data or not
        const updated = await updateCompanyData(company.id, scrapedData);
        if (updated) successCount++;
        
        // Prevent overwhelming servers - add delay between requests
        await new Promise(resolve => setTimeout(resolve, 3000));
      } catch (error) {
        console.error(`[${new Date().toISOString()}] Error processing company:`, error.message);
        easyNotificationsRequest("Erro na raspagem em"+process.env.SCRAP_UF, "error", "cadastrobr", "cadastrobr", error.message)
      }
    }
    
    console.log(`[${new Date().toISOString()}] Scraping complete. Updated ${successCount} out of ${companies.length} companies.`);

  /*   console.log(`[${new Date().toISOString()}] Connected to database successfully`);

   await pool.query('SELECT 4');
    
    const emails = await pool.query(`
      SELECT *
      FROM industrias 
      WHERE email IS NOT NULL AND uf = '${process.env.SCRAP_UF}'
    `);

    easyNotificationsRequest("Raspagem Finalizada em"+process.env.SCRAP_UF, "info", "cadastrobr", "cadastrobr", +emails.rows.length+" industrias com email")

    await pool.query('SELECT 5');

    const tel = await pool.query(`
      SELECT *
      FROM industrias 
      WHERE tel1 IS NOT NULL AND uf = '${process.env.SCRAP_UF}'
    `);

    easyNotificationsRequest("Raspagem Finalizada em"+process.env.SCRAP_UF, "info", "cadastrobr", "cadastrobr", +tel.rows.length+" industrias com telefone")

    const tel_email = await pool.query(`
      SELECT *
      FROM industrias 
      WHERE tel1 IS NOT NULL AND email IS NOT NULL AND uf = '${process.env.SCRAP_UF}'
    `);

    easyNotificationsRequest("Raspagem Finalizada em"+process.env.SCRAP_UF, "info", "cadastrobr", "cadastrobr", +tel_email.rows.length+" industrias com email e telefone")
    
    const total = await pool.query(`
      SELECT *
      FROM industrias 
      WHERE at IS NOT NULL AND uf = '${process.env.SCRAP_UF}'
    `);

    easyNotificationsRequest("Raspagem Finalizada em"+process.env.SCRAP_UF, "info", "cadastrobr", "cadastrobr", +total.rows.length+" total de instrias verificadas")
    */
  } catch (error) {
    console.error(`[${new Date().toISOString()}] FATAL ERROR:`, error.message);
    console.error(`[${new Date().toISOString()}] Stack trace:`, error.stack);
    easyNotificationsRequest("Erro na raspagem em"+process.env.SCRAP_UF, "error", "cadastrobr", "cadastrobr", error.message)

    process.exit(1);
  } finally {
    console.log(`[${new Date().toISOString()}] ===== WEB SCRAPER COMPLETED =====`);
  }
}

// Execute the main function
main();

module.exports = {
  getCompaniesForScraping,
  scrapeWebsite,
  updateCompanyData
};