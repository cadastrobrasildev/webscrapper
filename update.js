const { Pool } = require('pg');
const axios = require('axios'); // Using axios instead of node-fetch

// Configuração dos pools de conexão
const pool1 = new Pool({
    host: 'shortline.proxy.rlwy.net',
    port: 24642,
    database: 'railway',
    user: 'postgres',
    password: 'DSBlkKCnEUKRlGBbnyhofNcwkhwvINsp',
    connectionTimeoutMillis: 180000
});

const pool2 = new Pool({
    host: '93.127.135.79',
    database: 'rf_dados_publicos_cnpj',
    user: 'cadastrobr',
    password: 'cadastrobr1231*',
    port: 5432,
    connectionTimeoutMillis: 180000
});
console.log("v1.0.0")
// Helper function to fetch CNAE data from IBGE API
async function fetchCnaeData(cnaeCode) {
    try {
        if (!cnaeCode) return null;
        
        // Remove the last 2 digits to get the class code
        const classCode = cnaeCode.substring(0, cnaeCode.length - 2);
        
        const response = await axios.get(`https://servicodados.ibge.gov.br/api/v2/cnae/classes/${classCode}`);

        // Axios automatically throws errors for non-200 responses
        // Data is in response.data instead of using response.json()
        const data = response.data;

        return data;
    } catch (error) {
        console.error(`Error fetching CNAE data: ${error.message}`);
        return null;
    }
}

// Mapeamento completo de todas as colunas do Catálogo
const columnMapping = {
    // Dados básicos
    'cnpj': 'c.cnpj',
    'nome': 'cr.name',
    'fantasia': 'c.trade_name',
    
    // Endereço completo
    'endereco': 'c.address',
    'tipo_logradouro': 'c.address_type',
    'numero': 'c.address_number',
    'complemento': 'c.address_complement',
    'bairro': 'c.address_neighborhood',
    'cep': 'c.address_zip_code',
    'municipio': 'c.address_city_name',
    'uf': 'c.address_fu',
    
    // Contatos
    'ddd_telefone': 'c.tel1_dd',
    'telefone': 'c.tel1',
      // E-mails
    'email': 'c.email',
      
    // Atividades econômicas
    'cnae': 'c.cnae_main',
    'descricao': null,
    'exporta': 0,
    'importa': 0,
    
    // Produtos - updated to use dynamic data from API
    'produto_1': null, // Will be populated from API
    'produto_2': null, // Will be populated from API
    'produto_3': null, // Will be populated from API
    'outros_produtos': null,
    'materias_primas': null,
    
    // Funcionários e estrutura
    'nro_funcionarios':  null,
    'processo_produtivo': null,
    'ano_fundacao': `EXTRACT(YEAR FROM c.foundation_date)`,
    
    // Registros
    'inscricao': null,
    'matriz': 1,
    'filial': 0,
    
       // Redes sociais
    'pagina_web': null,
       
    // Controle interno
    'ativo': 1,
    '_token': null,
    'created_at': new Date(),
    'updated_at':  new Date(),
    
    // Dados complementares
    'natureza': null,
    'nomecontatoempresa': null,   
    'motivo': null,
    'ckmotivo': 0,
    'atualizado': 1,
    
    // Capital e porte
    'capital': 'cr.social_capital',
    'porte': 'cr.size_code',   
    
    'ativa': 'c.situation_desc'
};

async function syncCatalogo() {
    let processedCount = 0;
    const batchSize = 1; // Processando um registro por vez
    let hasMoreRecords = true;
    
    while (hasMoreRecords) {
        const client1 = await pool1.connect();
        const client2 = await pool2.connect();
        
        try {
            // Pega apenas um registro por vez que ainda não foi processado
            const resTransfer = await client1.query(
                `SELECT id, cnpj, email 
                FROM transfer 
                WHERE email <> '' AND email IS NOT NULL AND at='2'
                ORDER BY random()
                LIMIT $1`,
                [batchSize]
            );

            // Se não encontrou registros, encerra o loop
            if (resTransfer.rows.length === 0) {
                hasMoreRecords = false;
                console.log('Não há mais registros para processar.');
                break;
            }

            // Processa o registro encontrado
            for (const row of resTransfer.rows) {
                const cnpj = row.cnpj;
                const email = row.email;
                
                console.log(`Processando registro ${processedCount + 1}: CNPJ ${cnpj}`);
                
                // O resto do código de processamento permanece igual
                const resRF = await client2.query(`
                    SELECT 
                        c.*,
                        cr.*,
                        crs.*
                    FROM 
                        rf_company c
                        LEFT JOIN rf_company_root cr ON c.cnpj_root = cr.cnpj_root
                        LEFT JOIN rf_company_root_simples crs ON c.cnpj_root = crs.cnpj_root 
                    WHERE 
                        c.cnpj = $1`, [cnpj]);

                if (resRF.rows.length === 0) continue;

                const rfData = resRF.rows[0];
                
                // ... resto do código de processamento ...
                
                // Fetch CNAE data once per record
                let cnaeData = null;
                if (rfData.cnae_main) {
                    cnaeData = await fetchCnaeData(rfData.cnae_main);
                }
                
                // Construir objeto com todas as colunas
                const catalogoData = {};
                const updates = [];
                const values = [];
                let valueCount = 1;

                for (const [targetColumn, sourceExpression] of Object.entries(columnMapping)) {
                    let value = null;
                
                    try {
                        // Special handling for email field
                        if (targetColumn === 'email') {
                            value = email; // Use email from transfer table
                        } 
                        // Special handling for product fields based on CNAE data
                        else if (targetColumn === 'produto_1' && cnaeData) {
                            value = 'Industria';
                        }
                        else if (targetColumn === 'produto_2' && cnaeData && cnaeData.grupo) {
                            value = cnaeData.descricao;
                        }
                        else if (targetColumn === 'produto_3' && cnaeData && cnaeData.grupo && cnaeData.grupo.divisao) {
                            value = cnaeData.grupo.descricao;
                        }
                        else if (sourceExpression) {
                            // Rest of the existing code for other fields
                            if (typeof sourceExpression === 'string') {
                                // Handle SQL expressions
                                if (sourceExpression.startsWith('EXTRACT')) {
                                    if (rfData.foundation_date) {
                                        value = new Date(rfData.foundation_date).getFullYear();
                                    }
                                } else {
                                    // Only split if it's a string reference to a property
                                    const parts = sourceExpression.split('.');
                                    
                                    // Get table prefix (c, cr, crs)
                                    const tablePrefix = parts[0];
                                    
                                    // Get the actual property name without prefix
                                    const propertyName = parts[1];
                                    
                                    // Direct access to the property with appropriate casing
                                    value = rfData[propertyName];
                                    
                                    // For debugging the first record
                                    if (row === resTransfer.rows[0] && value === undefined) {
                                        console.log(`Field not found: ${targetColumn} -> ${sourceExpression}`);
                                        // Log available keys for this property
                                        console.log(`Available keys: ${Object.keys(rfData).filter(k => 
                                            k.toLowerCase() === propertyName.toLowerCase()).join(', ')}`);
                                    }
                                }
                            } else if (sourceExpression instanceof Date) {
                                // Handle Date objects directly
                                value = sourceExpression;
                            } else {
                                // Handle other types (numbers, booleans, etc.)
                                value = sourceExpression;
                            }
                        }
                    } catch (err) {
                        console.error(`Error extracting ${targetColumn} from ${sourceExpression}:`, err.message);
                    }

                    if (row === resTransfer.rows[0]) {
                        console.log(`${targetColumn} = ${value}`);
                    }

                    catalogoData[targetColumn] = value;
                    
                    updates.push(`${targetColumn} = $${valueCount}`);
                    values.push(value);
                    valueCount++;
                }

                // Check if the record exists
                const recordExists = await client1.query(
                    'SELECT * FROM catalogo WHERE cnpj = $1 LIMIT 1',
                    [cnpj]
                );

                if (recordExists.rows.length === 0) {
                    console.log(`CNPJ ${cnpj}: Inserindo novo registro`);
                    await client1.query(`
                        INSERT INTO catalogo (${Object.keys(columnMapping).join(', ')})
                        VALUES (${Object.keys(columnMapping).map((_, i) => `$${i + 1}`).join(', ')})`,
                        values
                    );
                } else {
                    console.log(`CNPJ ${cnpj}: Atualizando registro existente`);
                    await client1.query(`
                        UPDATE catalogo
                        SET ${updates.join(', ')} 
                        WHERE cnpj = $${valueCount}`,
                        [...values, cnpj]
                    );
                }

                // Update the status for processed record
                await client1.query(
                    'UPDATE transfer SET at = $1 WHERE id = $2',
                    ['4', row.id]
                );
                
                console.log(`CNPJ ${cnpj} sincronizado!`);
                console.log(`Status do registro ID ${row.id} atualizado para at=4`);
                
                processedCount++;
            }
            
            console.log(`Total de registros processados até agora: ${processedCount}`);
            
        } catch (error) {
            console.error('Erro durante o processamento:', error);
        } finally {
            client1.release();
            client2.release();
        }
    }
    
    console.log(`Processamento concluído. Total de registros processados: ${processedCount}`);
}

// Executar sincronização
syncCatalogo()
    .then(() => {
        console.log('Processo finalizado com sucesso');
        process.exit(0);
    })
    .catch((error) => {
        console.error('Falha crítica:', error);
        process.exit(1);
    });