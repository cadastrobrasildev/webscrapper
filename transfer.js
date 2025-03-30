const { Pool } = require('pg');
require('dotenv').config();
/**
 * Connects to database and retrieves manufacturing companies in SC state
 * with corporate email addresses.
 * @returns {Promise<Array>} Array of company records
 */
async function getManufacturingCompaniesInSC() {
  console.log(`[${new Date().toISOString()}] Attempting to connect to source database at 93.127.135.79...`);
  console.log("Transfer v1.0.4")
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
WHERE (
      c.cnae_main LIKE '10%' 
   OR c.cnae_main LIKE '11%' 
   OR c.cnae_main LIKE '12%' 
   OR c.cnae_main LIKE '13%' 
   OR c.cnae_main LIKE '14%' 
   OR c.cnae_main LIKE '15%' 
   OR c.cnae_main LIKE '16%' 
   OR c.cnae_main LIKE '17%' 
   OR c.cnae_main LIKE '18%' 
   OR c.cnae_main LIKE '19%' 
   OR c.cnae_main LIKE '20%' 
   OR c.cnae_main LIKE '21%' 
   OR c.cnae_main LIKE '22%' 
   OR c.cnae_main LIKE '23%' 
   OR c.cnae_main LIKE '24%' 
   OR c.cnae_main LIKE '25%' 
   OR c.cnae_main LIKE '26%' 
   OR c.cnae_main LIKE '27%' 
   OR c.cnae_main LIKE '28%' 
   OR c.cnae_main LIKE '29%' 
   OR c.cnae_main LIKE '30%' 
   OR c.cnae_main LIKE '31%' 
   OR c.cnae_main LIKE '32%' 
   OR c.cnae_main LIKE '33%')
  AND c.address_fu = '${process.env.TRANSFER_UF}' 
  AND c.situation_code = '02'
   AND cr.name !~ '^[0-9]' 
  AND cr.name !~ '[0-9]$'  
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