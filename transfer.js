const { Pool } = require('pg');

/**
 * Connects to database and retrieves manufacturing companies in SC state
 * with corporate email addresses.
 * @returns {Promise<Array>} Array of company records
 */
async function getManufacturingCompaniesInSC() {
  console.log(`[${new Date().toISOString()}] Attempting to connect to source database at 93.127.135.79...`);
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
        c.cnpj, c.email, c.tel1_dd, c.tel1 
      FROM 
        rf_company c
        LEFT JOIN rf_company_root cr ON c.cnpj_root = cr.cnpj_root
        LEFT JOIN rf_company_root_simples crs ON c.cnpj_root = crs.cnpj_root
        LEFT JOIN rf_company_tax_regime ctr ON c.cnpj_root = ctr.cnpj_root  
      WHERE 
        c.address_fu = 'SC' 
        AND c.situation_code = '02'
        AND CAST(LEFT(c.cnae_main, 2) AS INTEGER) BETWEEN 10 AND 33
        AND c.email NOT LIKE '%gmail%'
        AND c.email NOT LIKE '%yahoo%'
        AND c.email NOT LIKE '%hotmail%'
        AND c.email NOT LIKE '%outlook%'
        AND c.email NOT LIKE '%contab%'
        AND c.email NOT LIKE '%contabilidade%'
        AND c.email NOT LIKE '%contador%'
        AND c.email NOT LIKE '%icloud%'
        AND c.email NOT LIKE '%uol%'
        AND c.email NOT LIKE '%terra%'
        AND c.email NOT LIKE '%brturbo%'
        AND c.email NOT LIKE '%aol%'
        AND c.email NOT LIKE '%msn%'
        AND c.email NOT LIKE '%live%'
        AND c.email NOT LIKE '%protonmail%'
        AND c.email NOT LIKE '%zoho%'
        AND c.email NOT LIKE '%mail.com%'
        AND c.email NOT LIKE '%gmx%'
        AND c.email NOT LIKE '%yandex%'
        AND c.email NOT LIKE '%bol%'
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
    console.log(`[${new Date().toISOString()}] Checking if 'industrias' table exists...`);
    const tableCheck = await secondDBPool.query(`
      SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = 'industrias'
      ) as exists
    `);
    
    if (!tableCheck.rows[0].exists) {
      console.error(`[${new Date().toISOString()}] ERROR: 'industrias' table does not exist in target database`);
      return 0;
    }
    
    // Check table structure
    console.log(`[${new Date().toISOString()}] Checking 'industrias' table structure...`);
    const tableStructure = await secondDBPool.query(`
      SELECT column_name, data_type 
      FROM information_schema.columns 
      WHERE table_schema = 'public' 
      AND table_name = 'industrias'
    `);
    
    console.log(`[${new Date().toISOString()}] Table structure:`, tableStructure.rows);
    
    // Process each company in its own transaction, not in one big transaction
    console.log(`[${new Date().toISOString()}] Beginning data transfer of ${companies.length} records...`);
    
    for (const company of companies) {
      // Get a client for each company (each in its own transaction)
      const client = await secondDBPool.connect();
      
      try {
        // Skip companies without email
        if (!company.email) {
          console.log(`[${new Date().toISOString()}] Skipping company with no email: CNPJ=${company.cnpj}`);
          continue;
        }
        
        // Extract site from email (domain part after @)
        const site = company.email.split('@')[1];
        
        console.log(`[${new Date().toISOString()}] Processing company: CNPJ=${company.cnpj}, Email=${company.email}, Site=${site}`);
        
        // Begin transaction for this single record
        await client.query('BEGIN');
        
        // Insert into second database - no verification
        try {
          await client.query(
            `INSERT INTO industrias (cnpj, site) 
             VALUES ($1, $2)`,
            [company.cnpj, site]
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
    const countResult = await secondDBPool.query(`SELECT COUNT(*) FROM industrias`);
    console.log(`[${new Date().toISOString()}] Total records in industrias table: ${countResult.rows[0].count}`);
    
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