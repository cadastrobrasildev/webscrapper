// Instale as dependências:
// npm install puppeteer-extra puppeteer-extra-plugin-stealth random-useragent cheerio

const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const randomUseragent = require('random-useragent');
const cheerio = require('cheerio');
const { Pool } = require('pg');
const sleep = ms => new Promise(res => setTimeout(res, ms));
require('dotenv').config();

puppeteer.use(StealthPlugin());
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
    port: 5432, // Default PostgreSQL port
    connectionTimeoutMillis: 180000
});

(async () => {
    let browser;
    let captchaCounter = 0;
    
    async function startScraping() {
        try {
            // Define um user agent aleatório para ajudar a disfarçar a automação
            const userAgent = randomUseragent.getRandom();

            // Lança o navegador com configurações para evitar detecção
            browser = await puppeteer.launch({
                headless: true, // Mude para false se quiser visualizar o processo
                args: ['--no-sandbox', '--disable-setuid-sandbox']
            });

            const page = await browser.newPage();
            await page.setUserAgent(userAgent);

            // Bloqueia carregamento de imagens, estilos e fontes para agilizar a navegação
            await page.setRequestInterception(true);
            page.on('request', (req) => {
                if (['image', 'stylesheet', 'font'].includes(req.resourceType())) {
                    req.abort();
                } else {
                    req.continue();
                }
            });

            // Verifica conexão com banco de dados
            await pool1.query('SELECT 1');
            console.log(`[${new Date().toISOString()}] Connected to database successfully`);

            let recordsProcessed = 0;
            let keepSearching = true;

            while (keepSearching) {
                // Busca uma indústria aleatória que precisa de dados
                const result1 = await pool1.query(`SELECT id, cnpj, email, tel1 
                FROM industrias 
                WHERE at='3'
                ORDER BY random()
                LIMIT 1
                `);

                // Verifica se retornou algum resultado
                if (result1.rows.length === 0) {
                    console.log(`[${new Date().toISOString()}] No more records to process.`);
                    keepSearching = false;
                    break;
                }

                const bd1 = result1.rows[0];
                console.log(`[${new Date().toISOString()}] Processing CNPJ: ${bd1.cnpj}`);

                // Busca informações adicionais no segundo banco de dados
                const result2 = await pool2.query(`
                SELECT 
                c.trade_name, cr."name", c.address_fu, c.address_city_name 
                FROM 
                rf_company c
                LEFT JOIN rf_company_root cr ON c.cnpj_root = cr.cnpj_root
                LEFT JOIN rf_company_root_simples crs ON c.cnpj_root = crs.cnpj_root 
                WHERE 
                c.cnpj = '${bd1.cnpj}'
                `);

                if (result2.rows.length === 0) {
                    console.log(`[${new Date().toISOString()}] No additional info found for CNPJ: ${bd1.cnpj}`);
                    continue;
                }

                const bd2 = result2.rows[0];

                if (!bd2.trade_name && !bd2.name) {
                    console.log(`[${new Date().toISOString()}] Skipping record ID: ${bd1.id} - No trade_name or name available`);

                    // Atualiza o registro como processado para evitar que seja selecionado novamente
                    await pool1.query(`
                    UPDATE industrias
                    SET update_google = 1, at = 2
                    WHERE id = $1
                    `, [bd1.id]);

                    console.log(`[${new Date().toISOString()}] Marked record ID: ${bd1.id} as processed`);
                    continue; // Pula para o próximo registro
                }

                // Monta a consulta para o Google com base nas informações da empresa
                const empresaQuery = `${bd2.trade_name || bd2.name} ${bd2.address_city_name || ''} ${bd2.address_fu || ''}`;
                const query = `${empresaQuery} contato telefone email site`;
                const searchUrl = `https://www.google.com/search?q=${encodeURIComponent(query)}`;

                console.log(`[${new Date().toISOString()}] Searching Google for: ${query}`);

                // Acessa o Google e aguarda o carregamento completo da página
                await page.goto(searchUrl, { waitUntil: 'networkidle2' });
                // Aguarda um pouco para garantir que todos os conteúdos dinâmicos sejam carregados
                await sleep(3000);

                // Verifica se o Google apresentou um CAPTCHA
                const pageUrl = page.url();
                const pageContent = await page.content();
                const isCaptchaPresent =
                    pageUrl.includes('/sorry/') ||
                    pageUrl.includes('captcha') ||
                    pageContent.includes('unusual traffic') ||
                    pageContent.includes('verify you') ||
                    pageContent.includes('confirm you') ||
                    pageContent.includes('not a robot') ||
                    pageContent.includes('recaptcha');

                if (isCaptchaPresent) {
                    console.log(isCaptchaPresent)
                    console.error(`[${new Date().toISOString()}] CAPTCHA DETECTED! Attempt #${captchaCounter + 1}`);
                    captchaCounter++;
                    
                    // Fecha o browser atual
                    await browser.close();
                    console.log(`[${new Date().toISOString()}] Browser closed due to CAPTCHA.`);
                    
                    if (captchaCounter >= 3) {
                        console.log(`[${new Date().toISOString()}] CAPTCHA detected 3 times in a row. Waiting 3 minutes...`);
                        // Espera 3 minutos antes de tentar novamente
                        const captchaWaitTime = 3 * 60 * 1000; // 3 minutos em milissegundos
                        await sleep(captchaWaitTime);
                        captchaCounter = 0; // Reset counter after waiting
                    }
                    
                    // Reinicia o processo do começo
                    console.log(`[${new Date().toISOString()}] Restarting process from beginning...`);
                    return await startScraping();
                }

                // Obtém o conteúdo HTML da página
                const html = await page.content();
                const $ = cheerio.load(html);

                // Inicializa o objeto para armazenar os dados de contato
                let contato = {
                    telefone: null,
                    email: null,
                    site: null
                };

                // Define expressões regulares para encontrar telefone, email e website
                const regexTelefone = /(\(?\d{2}\)?\s?\d{4,5}-?\d{4})/g;
                const regexEmail = /([a-zA-Z0-9._-]+@[a-zA-Z0-9._-]+\.[a-zA-Z0-9._-]+)/g;
                const regexSite = /(https?:\/\/(?:www\.)?[a-zA-Z0-9.-]+\.[a-zA-Z]{2,6})/g;

                // Extrai todo o texto da página
                const pageText = $('body').text();

                // Tenta encontrar os dados usando as expressões regulares
                const matchTelefone = pageText.match(regexTelefone);
                const matchEmail = pageText.match(regexEmail);
                const matchSite = pageText.match(regexSite);

                let tel1_dd = null;
                let tel1 = null;

                if (matchTelefone && matchTelefone.length) {
                    // Extrai o telefone completo
                    const telefoneCompleto = matchTelefone[0];

                    // Extrai o DDD (assume formato "(XX)" ou "XX")
                    const dddMatch = telefoneCompleto.match(/\(?(\d{2})\)?/);
                    if (dddMatch && dddMatch[1]) {
                        tel1_dd = dddMatch[1];
                    }

                    // Extrai o número principal (remove o DDD, parênteses, espaços e hífens)
                    tel1 = telefoneCompleto.replace(/\(?\d{2}\)?[\s-]?/, '').replace(/[-\s]/g, '');

                    // Armazena no objeto contato para logging
                    contato.telefone = telefoneCompleto;
                    contato.tel1_dd = tel1_dd;
                    contato.tel1 = tel1;
                }

                if (matchEmail && matchEmail.length) {
                    // Verifica se o email é relacionado à empresa antes de aceitá-lo
                    const emailCandidate = matchEmail[0];
                    const emailParts = emailCandidate.split('@');

                    if (emailParts.length === 2) {
                        const domain = emailParts[1].toLowerCase();
                        // Remove o TLD (.com, .br, etc) para comparação
                        const domainName = domain.split('.')[0];

                        // Prepara o nome da empresa para comparação
                        const companyName = (bd2.name || bd2.trade_name || '').toLowerCase();

                        // Remove caracteres especiais e divide em palavras
                        const companyWords = companyName
                            .replace(/[^\w\s]/gi, '')
                            .split(/\s+/)
                            .filter(word => word.length > 2); // Ignora palavras muito curtas

                        // Verifica se alguma parte significativa do nome da empresa está no domínio
                        let isRelated = false;

                        // Caso 1: O domínio contém uma parte significativa do nome da empresa
                        for (const word of companyWords) {
                            if (word.length > 3 && domainName.includes(word)) {
                                isRelated = true;
                                console.log(`[${new Date().toISOString()}] Email validated - domain "${domainName}" contains company word "${word}"`);
                                break;
                            }
                        }

                        // Caso 2: O nome da empresa contém o domínio
                        if (!isRelated && domainName.length > 3 && companyName.includes(domainName)) {
                            isRelated = true;
                            console.log(`[${new Date().toISOString()}] Email validated - company name contains domain "${domainName}"`);
                        }

                        // Caso 3: Verifique por iniciais ou acronimos
                        if (!isRelated && companyWords.length > 1) {
                            const acronym = companyWords.map(word => word[0]).join('');
                            if (acronym.length > 2 && domainName.includes(acronym)) {
                                isRelated = true;
                                console.log(`[${new Date().toISOString()}] Email validated - domain contains company acronym "${acronym}"`);
                            }
                        }

                        if (isRelated) {
                            contato.email = emailCandidate;
                            console.log(`[${new Date().toISOString()}] Email accepted: ${contato.email}`);
                        } else {
                            console.log(`[${new Date().toISOString()}] Email rejected - not related to company: ${emailCandidate}`);
                        }
                    }
                }

                if (matchSite && matchSite.length) {
                    contato.site = matchSite[0];
                } else if (contato.email) {
                    // Extract website from email when no site was found
                    const emailParts = contato.email.split('@');
                    if (emailParts.length === 2 && emailParts[1].includes('.')) {
                        // Use domain part of email as website
                        contato.site = `https://www.${emailParts[1]}`;
                        console.log(`[${new Date().toISOString()}] Generated website from email: ${contato.site}`);
                    }
                }

                console.log(`[${new Date().toISOString()}] Contact data found:`, contato);

                // Cria o array de campos e valores para atualização
                const fieldsToUpdate = [];
                const valuesToUpdate = [];
                let paramIndex = 1;

                // Adiciona campos sempre que tiverem valores, independente do registro atual
                if (tel1_dd) {
                    fieldsToUpdate.push(`tel1_dd = $${paramIndex}`);
                    valuesToUpdate.push(tel1_dd);
                    paramIndex++;
                }

                if (tel1) {
                    fieldsToUpdate.push(`tel1 = $${paramIndex}`);
                    valuesToUpdate.push(tel1);
                    paramIndex++;
                }

                if (contato.email) {
                    fieldsToUpdate.push(`email = $${paramIndex}`);
                    valuesToUpdate.push(contato.email);
                    paramIndex++;
                }

              /*  if (contato.site) {
                    fieldsToUpdate.push(`site = $${paramIndex}`);
                    valuesToUpdate.push(contato.site);
                    paramIndex++;
                }*/

                // Sempre adiciona update_google = 1
                fieldsToUpdate.push(`update_google = 1`);
                fieldsToUpdate.push(`at = 2`);
                // Adiciona o ID como último parâmetro
                valuesToUpdate.push(bd1.id);

                // Atualiza o banco de dados somente se houver campos para atualizar
                if (fieldsToUpdate.length > 0) {
                    const updateQuery = `
                    UPDATE industrias
                    SET ${fieldsToUpdate.join(', ')}
                    WHERE id = $${paramIndex}
                    `;

                    try {
                        await pool1.query(updateQuery, valuesToUpdate);
                        console.log(`[${new Date().toISOString()}] Updated record ID: ${bd1.id}`);
                    } catch (error) {
                        console.error(`[${new Date().toISOString()}] Error updating record ID ${bd1.id}:`, error.message);
                    }
                }

                recordsProcessed++;
                console.log(`[${new Date().toISOString()}] Records processed: ${recordsProcessed}`);

                // Aguarda entre as pesquisas para não sobrecarregar o Google
                const waitTime = Math.floor(Math.random() * 5000) + 3000; // 3-8 segundos
                console.log(`[${new Date().toISOString()}] Waiting ${waitTime}ms before next search...`);
                await sleep(waitTime);
            }

            console.log(`[${new Date().toISOString()}] Script completed. Total records processed: ${recordsProcessed}`);

        } catch (error) {
            console.error(`[${new Date().toISOString()}] Error during scraping:`, error);
        } finally {
            // Garante que o browser seja fechado mesmo em caso de erro
            if (browser) {
                await browser.close();
                console.log(`[${new Date().toISOString()}] Browser closed.`);
            }

            // Fecha as conexões com o banco de dados
            await pool1.end();
            await pool2.end();
            console.log(`[${new Date().toISOString()}] Database connections closed.`);
        }
    }
    
    await startScraping();
})();
