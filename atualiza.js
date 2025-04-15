const { Pool } = require('pg');
const mysql = require('mysql2/promise');
const axios = require('axios');
require('dotenv').config();
console.log("v1.0.1");

// Configuração dos pools de conexão
const pool1 = new Pool({
    host: 'shortline.proxy.rlwy.net',
    port: 24642,
    database: 'railway',
    user: 'postgres',
    password: 'DSBlkKCnEUKRlGBbnyhofNcwkhwvINsp',
    connectionTimeoutMillis: 180000
});

const pool2 = mysql.createPool({
    host: 'sh-pro20.hostgator.com.br',
    user: 'eduard72_wp625',
    password: '37@S0DSm(p',
    database: 'eduard72_'+process.env.DB,
    port: 3306,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
});

async function testConnections() {
    try {
        await pool1.query('SELECT 1');
        console.log('Conexão pool1 OK');
        await pool2.query('SELECT 1');
        console.log('Conexão pool2 OK');
    } catch (err) {
        console.error('Erro ao testar conexões:', err);
        process.exit(1);
    }
}

async function transferCatalogo() {
    try {
        await testConnections();

        // Exclui todos os dados da tabela catalogo em pool2 antes de transferir
        console.log('Limpando tabela catalogo em pool2...');
        await pool2.query('DELETE FROM catalogo');
        console.log('Tabela catalogo limpa em pool2.');

        // 1. Busca todos os registros do pool1
        console.log('Buscando registros do pool1...');
        const { rows } = await pool1.query("SELECT * FROM catalogo WHERE uf='"+process.env.UF+"'");
        console.log(`Total de registros encontrados para inserir: ${rows.length}`);
        if (rows.length === 0) {
            console.log('Nenhum registro encontrado no pool1. Encerrando.');
            return;
        }

        let countTentados = 0;
        let countInseridos = 0;
        for (const row of rows) {
            const keys = Object.keys(row);
            const values = Object.values(row);

            // Log para cada registro antes de inserir
            console.log(`Tentando inserir registro id_catalogo=${row.id_catalogo}`);

            const insertQuery = `
                INSERT INTO catalogo (${keys.join(',')})
                VALUES (${keys.map(() => '?').join(',')})
                ON DUPLICATE KEY UPDATE
                ${keys.filter(k => k !== 'id_catalogo').map(k => `${k}=VALUES(${k})`).join(', ')}
            `;

            countTentados++;
            try {
                const [result] = await pool2.query(insertQuery, values);
                if (result.affectedRows > 0) {
                    countInseridos++;
                }
                // Log do resultado da inserção
                console.log(`Registro id_catalogo=${row.id_catalogo} inserido/atualizado. Total inseridos: ${countInseridos}/${countTentados}`);
            } catch (err) {
                console.error(`Erro ao inserir id_catalogo=${row.id_catalogo}:`, err.message);
            }
        }

        console.log(`Transferência concluída! Registros tentados: ${countTentados}, inseridos/atualizados: ${countInseridos}`);
    } catch (err) {
        console.error('Erro na transferência:', err);
    } finally {
        await pool1.end();
        await pool2.end();
    }
}

transferCatalogo();