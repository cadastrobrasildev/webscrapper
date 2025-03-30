const { Pool } = require('pg');
require('dotenv').config();
/**
 * Connects to database and retrieves manufacturing companies in SC state
 * with corporate email addresses.
 * @returns {Promise<Array>} Array of company records
 */
async function getManufacturingCompaniesInSC() {
  console.log(`[${new Date().toISOString()}] Attempting to connect to source database at 93.127.135.79...`);
  console.log("v1.0.1")
  // Create a connection pool
  const pool = new Pool({
    host: '93.127.135.79',
    database: 'rf_dados_publicos_cnpj',
    user: 'cadastrobr',
    password: 'cadastrobr1231*',
    port: 5432, // Default PostgreSQL port
    connectionTimeoutMillis: 15000 // 15 seconds timeout
  });

  try {
    console.log(`[${new Date().toISOString()}] Connection pool created, testing connection...`);
    // Test connection
    await pool.query('SELECT 1');
    console.log(`[${new Date().toISOString()}] Successfully connected to source database`);
    
    console.log(`[${new Date().toISOString()}] Executing manufacturing companies query...`);
    // Execute query
    const result = await pool.query(`
     SELECT 
   c.*
FROM 
    rf_company c
    LEFT JOIN rf_company_root cr ON c.cnpj_root = cr.cnpj_root
    LEFT JOIN rf_company_root_simples crs ON c.cnpj_root = crs.cnpj_root
    LEFT JOIN rf_company_tax_regime ctr ON c.cnpj_root = ctr.cnpj_root  
WHERE c.cnae_main IN (
    '1529700','1741902','1749400','2211100','2219600','2399199','2522500','2539001','2543800','2593400',
    '2621300','2622100','2651500','2670102','2710401','2751100','2759701','2759799','2790201','2790299',
    '2811900','2815102','2821601','2822401','2822402','2823200','2825900','2829101','2829199','2833300',
    '2840200','2851800','2852600','2854200','2861500','2862300','2863100','2864000','2865800','2866600',
    '2869100','3101200','3103900','3291400','3299002','3299099','3312102','3313999','3314701','3314707',
    '3314708','3314709','3314710','3314711','3314713','3314714','3314715','3314717','3314718','3314719',
    '3314720','3314721','3314722','3314799','3321000','2019399','2029100','2422901','2449199','2599399',
    '2443100','2452100','2051700','2441501','2441502','2512800','2532201','2542000','2591800','2592601',
    '2592602','2733300','2229301','2229302','2229303','2229399','2541100','2640000','2740602','2930103',
    '3104700','3230200','3292201','3292202','1733800','3240099'
)
AND c.address_fu = '${process.env.TRANSFER_UF}' 
  AND c.situation_code = '02'
ORDER BY random()
LIMIT 30000
    `);
    
    
    console.log(`[${new Date().toISOString()}] Query executed successfully. Retrieved ${result.rows.length} companies`);
    if (result.rows.length > 0) {
      console.log(`[${new Date().toISOString()}] Sample company data:`, JSON.stringify(result.rows[0]).substring(0, 200) + '...');
    }
    return result.rows;
  } catch (error) {
    console.error(`[${new Date().toISOString()}] Database query error:`, error);
    console.error(`[${new Date().toISOString()}] Error details:`, {
      message: error.message,
      stack: error.stack,
      code: error.code,
      detail: error.detail
    });
    throw error;
  } finally {
    console.log(`[${new Date().toISOString()}] Closing source database connection pool...`);
    try {
      await pool.end();
      console.log(`[${new Date().toISOString()}] Source database connection pool closed successfully`);
    } catch (err) {
      console.error(`[${new Date().toISOString()}] Error closing source database connection:`, err.message);
    }
  }
}

/**
 * Transfers company data from the first database to the second database
 * @returns {Promise<number>} Number of records inserted
 */
async function transferCompaniesToSecondDB() {
  console.log(`[${new Date().toISOString()}] ===== STARTING DATA TRANSFER BETWEEN DATABASES =====`);
  
  // Get data from the first database
  console.log(`[${new Date().toISOString()}] Retrieving companies from source database...`);
  const companies = await getManufacturingCompaniesInSC();
  console.log(`[${new Date().toISOString()}] Retrieved ${companies.length} companies from source database`);
  
  if (companies.length === 0) {
    console.log(`[${new Date().toISOString()}] No data to transfer. Exiting transfer process.`);
    return 0;
  }
  
  // Connect to the second database
  console.log(`[${new Date().toISOString()}] Attempting to connect to target database at shortline.proxy.rlwy.net:24642...`);
  const secondDBPool = new Pool({
    host: 'shortline.proxy.rlwy.net',
    port: 24642,
    database: 'railway',
    user: 'postgres',
    password: 'DSBlkKCnEUKRlGBbnyhofNcwkhwvINsp',
    connectionTimeoutMillis: 15000 // 15 seconds timeout
  });
  
  let insertedCount = 0;
  
  try {
    // Test connection to second database
    console.log(`[${new Date().toISOString()}] Testing connection to target database...`);
    await secondDBPool.query('SELECT 1');
    console.log(`[${new Date().toISOString()}] Successfully connected to target database`);
    
    // Check if the industrias table exists
    console.log(`[${new Date().toISOString()}] Checking if 'transfer' table exists...`);
    const tableCheck = await secondDBPool.query(`
      SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = 'transfer'
      ) as exists
    `);
    
    if (!tableCheck.rows[0].exists) {
      console.error(`[${new Date().toISOString()}] ERROR: 'transfer' table does not exist in target database`);
      return 0;
    }
    
    // Check table structure
    console.log(`[${new Date().toISOString()}] Checking 'transfer' table structure...`);
    const tableStructure = await secondDBPool.query(`
      SELECT column_name, data_type 
      FROM information_schema.columns 
      WHERE table_schema = 'public' 
      AND table_name = 'transfer'
    `);
    
    console.log(`[${new Date().toISOString()}] Table structure:`, tableStructure.rows);
    
    // Process each company in its own transaction, not in one big transaction
    console.log(`[${new Date().toISOString()}] Beginning data transfer of ${companies.length} records...`);
    
    for (const company of companies) {
      // Get a client for each company (each in its own transaction)
      const client = await secondDBPool.connect();
      
      try {            
       
        console.log(`[${new Date().toISOString()}] Processing company: CNPJ=${company.cnpj}, Email=${company.email}`);
        
        // Begin transaction for this single record
        await client.query('BEGIN');
        
        // Check if CNPJ already exists in the transfer table
        const existsCheck = await client.query(
          `SELECT EXISTS (SELECT 1 FROM transfer WHERE cnpj = $1) as exists`,
          [company.cnpj]
        );
        
        if (existsCheck.rows[0].exists) {
          // CNPJ already exists, skip insertion
          console.log(`[${new Date().toISOString()}] Skipping CNPJ ${company.cnpj} - already exists in database`);
          await client.query('COMMIT');
          continue;
        }
        
        //console.log(company)
        // Insert into second database - after verification
        try {
          await client.query(
            `INSERT INTO transfer (cnpj, uf, at) 
             VALUES ($1, '${process.env.TRANSFER_UF}', '1')`,
            [company.cnpj]
          );
          
          // Commit transaction immediately after insert
          await client.query('COMMIT');
          console.log(`[${new Date().toISOString()}] Inserted CNPJ ${company.cnpj}`);
          insertedCount++;
          
          // Log progress
          if (insertedCount % 10 === 0) {
            console.log(`[${new Date().toISOString()}] Progress: Transferred ${insertedCount}/${companies.length} records (${Math.round(insertedCount/companies.length*100)}%)`);
          }
        } catch (insertError) {
          // Rollback on error
          await client.query('ROLLBACK');
          
          // Ignore duplicate key errors, but log other errors
          if (insertError.code !== '23505') { // 23505 is the error code for unique violation
            console.error(`[${new Date().toISOString()}] Error inserting:`, insertError.message);
          }
        }
      } catch (recordError) {
        // Rollback transaction for this record on error
        try {
          await client.query('ROLLBACK');
        } catch (rollbackError) {
          console.error(`[${new Date().toISOString()}] Error during rollback:`, rollbackError.message);
        }
        
        console.error(`[${new Date().toISOString()}] Error inserting record:`, {
          cnpj: company.cnpj,
          email: company.email,
          tel1_dd: company.tel1_dd,
          tel1: company.tel1,
          error: recordError.message,
          errorCode: recordError.code,
          detail: recordError.detail
        });
      } finally {
        // Release client back to the pool
        client.release();
      }
    }
    
    // Double-check total record count    
    console.log(`[${new Date().toISOString()}] Successfully transferred ${insertedCount} records to second database`);
    return insertedCount;
    
  } catch (error) {
    console.error(`[${new Date().toISOString()}] Error transferring data:`, error);
    console.error(`[${new Date().toISOString()}] Error details:`, {
      message: error.message,
      stack: error.stack,
      code: error.code,
      detail: error.detail
    });
    throw error;
  } finally {
    // Close the connection
    await secondDBPool.end();
    console.log(`[${new Date().toISOString()}] Second database connection closed`);
  }
}

// Update main function with better error handling
async function main() {
  console.log(`[${new Date().toISOString()}] ===== APPLICATION STARTED =====`);
  try {
    // Transfer data to second database
    const insertCount = await transferCompaniesToSecondDB();
    console.log(`[${new Date().toISOString()}] Transfer complete: ${insertCount} records inserted`);
  } catch (error) {
    console.error(`[${new Date().toISOString()}] FATAL ERROR:`, error.message);
    console.error(`[${new Date().toISOString()}] Stack trace:`, error.stack);
    process.exit(1); // Exit with error code
  } finally {
    console.log(`[${new Date().toISOString()}] ===== APPLICATION COMPLETED =====`);
  }
}

// Execute the main function
main();

module.exports = { 
  getManufacturingCompaniesInSC,
  transferCompaniesToSecondDB
};