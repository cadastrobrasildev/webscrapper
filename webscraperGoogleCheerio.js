// Instale as dependências:
// npm install axios cheerio random-useragent

const axios = require('axios');
const cheerio = require('cheerio');
const randomUseragent = require('random-useragent');
const { Pool } = require('pg');
const sleep = ms => new Promise(res => setTimeout(res, ms));
const ProxyChain = require('proxy-chain');
require('dotenv').config();

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
console.log("v1.0.7")
/**
 * Função para extrair informações de telefone de um texto
 * @param {string} texto - O texto contendo números de telefone
 * @returns {Object|null} - Objeto com informações de telefone ou null se não encontrado
 */
function extrairTelefone(texto) {
    // Expressão regular para encontrar telefones no formato brasileiro
    const regexTelefone = /(\(?\d{2}\)?\s*\d{4,5}[-.\s]?\d{4})/g;
    
    // Encontra todos os telefones no texto
    const telefones = texto.match(regexTelefone);
    
    if (!telefones || telefones.length === 0) {
        return null;
    }
    
    // Pega o primeiro telefone encontrado
    const telefoneCompleto = telefones[0];
    console.log(`[${new Date().toISOString()}] Telefone encontrado (bruto): ${telefoneCompleto}`);
    
    // Extrai apenas os dígitos do telefone
    const apenasDigitos = telefoneCompleto.replace(/\D/g, '');
    console.log(`[${new Date().toISOString()}] Telefone apenas dígitos: ${apenasDigitos}`);
    
    // Verifica se o número tem pelo menos 10 dígitos (DDD + número)
    if (apenasDigitos.length < 10) {
        console.log(`[${new Date().toISOString()}] Telefone inválido: menos de 10 dígitos`);
        return null;
    }
    
    // Extrai o DDD (os dois primeiros dígitos)
    const ddd = apenasDigitos.substring(0, 2);
    
    // Extrai o número sem o DDD
    const numero = apenasDigitos.substring(2);
    
    console.log(`[${new Date().toISOString()}] DDD extraído: ${ddd}, Número: ${numero}`);
    
    return {
        telefoneCompleto: telefoneCompleto,
        tel1_dd: ddd,
        tel1: numero
    };
}

/**
 * Função para extrair endereços de e-mail de um texto, incluindo aqueles que podem estar ofuscados
 * @param {string} texto - O texto contendo endereços de e-mail
 * @returns {string|null} - O e-mail encontrado ou null se não encontrado
 */
function extrairEmail(texto) {
    // Limpa entidades HTML antes de iniciar a busca
    const textoLimpo = texto.replace(/&(#[0-9]+|[a-z]+);/gi, match => {
        if (match === '&amp;') return '&';
        if (match === '&lt;') return '<';
        if (match === '&gt;') return '>';
        if (match === '&quot;') return '"';
        if (match === '&#64;') return '@';
        if (match === '&#46;') return '.';
        if (match.startsWith('&#')) {
            const code = parseInt(match.substring(2, match.length - 1));
            return String.fromCharCode(code);
        }
        return match;
    });
    
    // Expressão regular melhorada para encontrar e-mails
    const regexEmail = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;
    
    // Encontra todos os e-mails no texto
    const emails = textoLimpo.match(regexEmail);
    
    if (!emails || emails.length === 0) {
        console.log(`[${new Date().toISOString()}] Nenhum e-mail encontrado no texto com regex padrão`);
        
        // Padrões de ofuscação comuns
        const padroes = [
            // contato (at) dominio (dot) com
            {
                regex: /([a-zA-Z0-9._%+-]+)[\s]*[\[\(]?(?:at|arroba|@)[\]\)]?[\s]*([a-zA-Z0-9.-]+)[\s]*[\[\(]?(?:dot|ponto|\.)[\]\)]?[\s]*([a-zA-Z]{2,})/gi,
                construtor: (m, p1, p2, p3) => `${p1.trim()}@${p2.trim()}.${p3.trim()}`
            },
            // contato[at]dominio[dot]com
            {
                regex: /([a-zA-Z0-9._%+-]+)[\s]*\[(?:at|arroba|@)\][\s]*([a-zA-Z0-9.-]+)[\s]*\[(?:dot|ponto|\.)\][\s]*([a-zA-Z]{2,})/gi,
                construtor: (m, p1, p2, p3) => `${p1.trim()}@${p2.trim()}.${p3.trim()}`
            },
            // contato[at]dominio.com
            {
                regex: /([a-zA-Z0-9._%+-]+)[\s]*\[(?:at|arroba|@)\][\s]*([a-zA-Z0-9.-]+\.[a-zA-Z]{2,})/gi,
                construtor: (m, p1, p2) => `${p1.trim()}@${p2.trim()}`
            },
            // contato(at)dominio.com
            {
                regex: /([a-zA-Z0-9._%+-]+)[\s]*\((?:at|arroba|@)\)[\s]*([a-zA-Z0-9.-]+\.[a-zA-Z]{2,})/gi,
                construtor: (m, p1, p2) => `${p1.trim()}@${p2.trim()}`
            },
            // contato AT dominio DOT com
            {
                regex: /([a-zA-Z0-9._%+-]+)[\s]+(?:AT|ARROBA)[\s]+([a-zA-Z0-9.-]+)[\s]+(?:DOT|PONTO)[\s]+([a-zA-Z]{2,})/gi,
                construtor: (m, p1, p2, p3) => `${p1.trim()}@${p2.trim()}.${p3.trim()}`
            }
        ];
        
        // Tenta cada padrão de ofuscação
        for (const padrao of padroes) {
            const matches = [...textoLimpo.matchAll(padrao.regex)];
            if (matches && matches.length > 0) {
                const match = matches[0];
                const emailReconstruido = padrao.construtor(...match);
                
                // Verifica se o email reconstruído parece válido
                if (emailReconstruido.match(regexEmail)) {
                    console.log(`[${new Date().toISOString()}] E-mail ofuscado reconstruído: ${emailReconstruido}`);
                    return emailReconstruido;
                }
            }
        }
        
        return null;
    }
    
    console.log(`[${new Date().toISOString()}] ${emails.length} e-mails encontrados no texto`);
    
    // Lista de domínios de serviços de e-mail conhecidos
    const dominiosGenericos = [
        'gmail.com', 'hotmail.com', 'outlook.com', 'yahoo.com', 
        'live.com', 'icloud.com', 'aol.com', 'mail.com',
        'protonmail.com', 'yandex.com', 'zoho.com', 'uol.com.br',
        'bol.com.br', 'terra.com.br', 'globo.com', 'ig.com.br'
    ];
    
    // Lista de prefixos de e-mail genéricos
    const emailsGenericos = [
        'info@', 'contato@', 'contact@', 'mail@', 'email@',
        'support@', 'suporte@', 'noreply@', 'no-reply@',
        'newsletter@', 'news@', 'marketing@', 'webmaster@',
        'admin@', 'administrador@', 'administrator@',
        'help@', 'ajuda@', 'sac@', 'atendimento@',
        'comercial@', 'vendas@', 'sales@', 'fiscal@'
    ];
    
    // Filtrar emails válidos (eliminar possíveis falsos positivos)
    const emailsValidos = emails.filter(email => {
        // Verifica formato básico
        if (!/^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$/.test(email)) {
            return false;
        }
        
        // Verifica tamanho mínimo e máximo
        if (email.length < 6 || email.length > 254) {
            return false;
        }
        
        // Verifica se domínio tem pelo menos um caractere antes do ponto
        const dominioParts = email.split('@')[1].split('.');
        if (dominioParts.some(part => part.length === 0)) {
            return false;
        }
        
        return true;
    });
    
    if (emailsValidos.length === 0) {
        return null;
    }
    
    // Primeiro, procura por emails com domínio específico (não de serviços comuns)
    for (const email of emailsValidos) {
        const dominio = email.split('@')[1].toLowerCase();
        
        if (!dominiosGenericos.includes(dominio) && 
            !emailsGenericos.some(prefix => email.toLowerCase().startsWith(prefix))) {
            console.log(`[${new Date().toISOString()}] E-mail de domínio específico encontrado: ${email}`);
            return email;
        }
    }
    
    // Segundo, procura por emails com domínio específico mesmo com prefixo genérico
    for (const email of emailsValidos) {
        const dominio = email.split('@')[1].toLowerCase();
        
        if (!dominiosGenericos.includes(dominio)) {
            console.log(`[${new Date().toISOString()}] E-mail com domínio específico encontrado (mesmo com prefixo genérico): ${email}`);
            return email;
        }
    }
    
    // Terceiro, procura por qualquer email não genérico
    for (const email of emailsValidos) {
        if (!emailsGenericos.some(prefix => email.toLowerCase().startsWith(prefix))) {
            console.log(`[${new Date().toISOString()}] E-mail não genérico encontrado: ${email}`);
            return email;
        }
    }
    
    // Por fim, retorna o primeiro email válido da lista
    console.log(`[${new Date().toISOString()}] Retornando primeiro e-mail disponível: ${emailsValidos[0]}`);
    return emailsValidos[0];
}

/**
 * Função para verificar se um domínio é de um provedor de email conhecido
 * @param {string} site - O domínio do site para verificar
 * @returns {boolean} - Verdadeiro se for um provedor de email conhecido
 */
function isEmailProviderDomain(site) {
    // Lista de domínios de provedores de email conhecidos
    const emailProviders = [
        'gmail.com',
        'hotmail.com',
        'outlook.com',
        'yahoo.com',
        'live.com',
        'icloud.com',
        'aol.com',
        'mail.com',
        'protonmail.com',
        'yandex.com',
        'zoho.com',
        'gmx.com',
        'tutanota.com',
        'inbox.com',
        'terra.com.br',
        'uol.com.br',
        'bol.com.br',
        'ig.com.br',
        'globo.com'
    ];
    
    // Limpa o site para comparação (remove http://, https://, www.)
    let cleanSite = site.toLowerCase();
    cleanSite = cleanSite.replace(/^https?:\/\//i, '');
    cleanSite = cleanSite.replace(/^www\./i, '');
    cleanSite = cleanSite.trim();
    
    // Verifica se o site termina com qualquer um dos domínios de email
    return emailProviders.some(provider => cleanSite === provider || cleanSite.endsWith('.' + provider));
}

/**
 * Verifica se um email está na lista negra
 * @param {string} email - O email para verificar
 * @returns {boolean} - True se o email estiver na lista negra, false caso contrário
 */
function isBlacklistedEmail(email) {
    if (!email) return true;
    
    // Lista de emails específicos na lista negra
    const blacklistedEmails = [
        'sac@empresa.com.br',
        'no.reply@empresa.com.br',
        'naoresponda@empresa.com.br',
        'contato@dominio.com.br',
        'contato@site.com.br'
    ];
    
    // Lista de domínios na lista negra
    const blacklistedDomains = [
        'gst@ic.com',
        'a.com.br',
        'ic.com',
        'tempmail.com',
        'guerrillamail.com',
        'disposable.com',
        'yopmail.com',
        'mailcatch.com',
        'getnada.com',
        'sharklasers.com',
        'example.com',
        'domain.com',
        'site.com.br',
        'dominio.com.br',
        'email.com'
    ];
    
    // Lista de padrões de email na lista negra
    const blacklistedPatterns = [
        /^info@/i,
        /^noreply@/i,
        /^no-reply@/i,
        /^donotreply@/i,
        /^naoresponda@/i,
        /^naorespondaeste@/i,
        /^sac@/i,
        /^ouvidoria@/i,
        /^fiscal@/i,
        /^cancelamento@/i,
        /^fake@/i,
        /^teste@/i,
        /^test@/i,
        /^example@/i,
        /^admin@/i,
        /^gts@/i,
        /^econod@/i
    ];
    
    // Verifica se o email está na lista de emails específicos
    if (blacklistedEmails.includes(email.toLowerCase())) {
        console.log(`[${new Date().toISOString()}] Email na lista negra: ${email}`);
        return true;
    }
    
    // Verifica se o domínio do email está na lista negra
    const domain = email.split('@')[1].toLowerCase();
    if (blacklistedDomains.some(blacklistedDomain => domain === blacklistedDomain || domain.endsWith(`.${blacklistedDomain}`))) {
        console.log(`[${new Date().toISOString()}] Domínio de email na lista negra: ${domain}`);
        return true;
    }
    
    // Verifica se o email corresponde a algum padrão na lista negra
    if (blacklistedPatterns.some(pattern => pattern.test(email))) {
        console.log(`[${new Date().toISOString()}] Email com padrão na lista negra: ${email}`);
        return true;
    }
    
    return false;
}

/**
 * Função para obter um proxy aleatório de uma lista de servidores proxy
 * @returns {Promise<string|null>} - URL do proxy ou null se nenhum proxy válido for encontrado
 */
async function getRandomProxy() {
    // Lista de servidores proxy (substitua com seus próprios proxies ou um serviço de proxy)
    const proxyList = [
        'http://username:password@proxy1.example.com:8080',
        'http://username:password@proxy2.example.com:8080',
        'http://username:password@proxy3.example.com:8080',
        // Adicione mais proxies à sua lista
    ];
    
    // Também pode obter proxies de um serviço online
    try {
        // Tenta buscar uma lista de proxies gratuitos de um serviço
        const response = await axios.get('https://api.proxyscrape.com/v2/?request=getproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all');
        const proxyData = response.data;
        
        if (typeof proxyData === 'string') {
            // Adiciona proxies obtidos à lista
            const newProxies = proxyData
                .split('\n')
                .filter(proxy => proxy.trim().length > 0)
                .map(proxy => `http://${proxy.trim()}`);
                
            proxyList.push(...newProxies);
            console.log(`[${new Date().toISOString()}] Added ${newProxies.length} proxies from online service`);
        }
    } catch (error) {
        console.log(`[${new Date().toISOString()}] Failed to fetch proxies from online service: ${error.message}`);
        // Continua com a lista de proxies existente
    }
    
    // Escolhe um proxy aleatório da lista
    if (proxyList.length === 0) {
        console.log(`[${new Date().toISOString()}] No proxies available`);
        return null;
    }
    
    const randomProxy = proxyList[Math.floor(Math.random() * proxyList.length)];
    
    try {
        // Verifica se o proxy está funcionando
        console.log(`[${new Date().toISOString()}] Testing proxy: ${randomProxy}`);
        
        // Usa o proxy-chain para autenticação
        const newProxyUrl = await ProxyChain.anonymizeProxy(randomProxy);
        
        // Testa o proxy com uma requisição simples
        const testResponse = await axios.get('https://httpbin.org/ip', {
            proxy: false,  // Desativa o proxy padrão do axios
            httpsAgent: new require('https').Agent({ 
                proxy: newProxyUrl,
                timeout: 5000,
                rejectUnauthorized: false
            }),
            timeout: 10000,
        });
        
        console.log(`[${new Date().toISOString()}] Proxy test successful: ${JSON.stringify(testResponse.data)}`);
        return newProxyUrl;
    } catch (error) {
        console.log(`[${new Date().toISOString()}] Proxy test failed: ${error.message}`);
        // Tenta com outro proxy recursivamente, mas com limite para evitar recursão infinita
        return null;
    }
}

/**
 * Cria uma instância do axios com um proxy aleatório
 * @returns {Promise<Object>} - Instância do axios configurada com proxy
 */
async function createAxiosWithProxy() {
    const userAgent = randomUseragent.getRandom();
    const config = {
        headers: {
            'User-Agent': userAgent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0'
        },
        timeout: 30000,
        maxRedirects: 5
    };
    
    // Tenta obter um proxy válido
    const proxyUrl = await getRandomProxy();
    
    if (proxyUrl) {
        console.log(`[${new Date().toISOString()}] Using proxy: ${proxyUrl}`);
        // Configura o axios com o proxy
        config.proxy = false; // Desativa o proxy padrão do axios
        config.httpsAgent = new require('https').Agent({
            proxy: proxyUrl,
            rejectUnauthorized: false // Permite certificados auto-assinados
        });
    } else {
        console.log(`[${new Date().toISOString()}] No working proxy found, proceeding without proxy`);
    }
    
    return axios.create(config);
}

(async () => {
    let captchaCounter = 0;
    
    async function startScraping() {
        try {
            // Define um user agent aleatório para ajudar a disfarçar a automação
            const userAgent = randomUseragent.getRandom();

            // Configura o cliente axios com headers que parecem com navegador normal
            const axiosInstance = axios.create({
                headers: {
                    'User-Agent': userAgent,
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'DNT': '1',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                    'Cache-Control': 'max-age=0'
                },
                timeout: 30000
            });

            // Verifica conexão com banco de dados
            await pool1.query('SELECT 1');
            console.log(`[${new Date().toISOString()}] Connected to database successfully`);

            let recordsProcessed = 0;
            let keepSearching = true;

            while (keepSearching) {
                // Inicializa variáveis de contato no início de cada iteração
                let contato = {
                    telefone: null,
                    email: null,
                    site: null,
                    tel1_dd: null,
                    tel1: null
                };
                
                // Adicionar um flag para rastrear erros durante o processamento
                let hasProcessingError = false;
                
                // Busca uma indústria aleatória que precisa de dados
                const result1 = await pool1.query(`SELECT id, cnpj, site, email, tel1_dd, tel1
                FROM transfer 
                WHERE at = '1'
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
                // Inicializa contato com valores do banco, se existirem
                if (bd1.site) contato.site = bd1.site;
                if (bd1.email) contato.email = bd1.email;
                if (bd1.tel1_dd) contato.tel1_dd = bd1.tel1_dd;
                if (bd1.tel1) contato.tel1 = bd1.tel1;
                
                // Flag para indicar se precisamos buscar no Google
                let needGoogleSearch = true;
                
                // Se já temos um site, vamos scrapear primeiro
                if (bd1.site) {
                    // Verifica se o site parece ser um domínio de provedor de email
                    if (isEmailProviderDomain(bd1.site)) {
                        console.log(`[${new Date().toISOString()}] Site ${bd1.site} appears to be an email provider domain. Skipping direct scraping.`);
                        needGoogleSearch = true;
                    } else {
                        console.log(`[${new Date().toISOString()}] Site already exists in database: ${bd1.site}. Scraping it first.`);
                        
                        try {
                            // Define um user agent aleatório
                            const userAgent = randomUseragent.getRandom();
                            const axiosInstance = axios.create({
                                headers: {
                                    'User-Agent': userAgent,
                                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                                    'Accept-Language': 'en-US,en;q=0.5',
                                    'Accept-Encoding': 'gzip, deflate, br',
                                    'DNT': '1',
                                    'Connection': 'keep-alive',
                                    'Upgrade-Insecure-Requests': '1',
                                    'Cache-Control': 'max-age=0'
                                },
                                timeout: 15000,
                                maxRedirects: 5,
                                validateStatus: function (status) {
                                    return status >= 200 && status < 300;
                                }
                            });
                            
                            // Visita o site da empresa
                            const siteResponse = await axiosInstance.get("https://"+bd1.site);
                            const siteHtml = siteResponse.data;
                            const $site = cheerio.load(siteHtml);
                            
                            // Extrai todo o texto do site
                            const siteText = $site('body').text();
                            
                            // Verifica se o site tem conteúdo mínimo
                            if (siteText.length > 100) {
                                // Busca telefone se ainda não temos
                                if (!contato.tel1 || !contato.tel1_dd) {
                                    const infoTelefone = extrairTelefone(siteText);
                                    if (infoTelefone) {
                                        contato.telefone = infoTelefone.telefoneCompleto;
                                        contato.tel1_dd = infoTelefone.tel1_dd;
                                        contato.tel1 = infoTelefone.tel1;
                                        console.log(`[${new Date().toISOString()}] Telefone encontrado no site da empresa: ${contato.telefone}, DDD: ${contato.tel1_dd}, Número: ${contato.tel1}`);
                                    }
                                }
                                
                                // Busca email se ainda não temos - MODIFICAÇÃO AQUI
                                if (!contato.email) {
                                    // Extrai texto de todo o site
                                    const siteText = $site('').text();
                                    
                                    // Tenta extrair email do texto principal
                                    const emailEncontrado = extrairEmail(siteText);
                                    if (emailEncontrado && !isBlacklistedEmail(emailEncontrado)) {
                                        contato.email = emailEncontrado;
                                        console.log(`[${new Date().toISOString()}] Email encontrado no texto do site: ${contato.email}`);
                                    } else if (emailEncontrado) {
                                        console.log(`[${new Date().toISOString()}] Email encontrado em lista negra, ignorado: ${emailEncontrado}`);
                                    }
                                }
                                
                                // Se já encontramos telefone e email, não precisamos buscar no Google
                                if (contato.tel1 && contato.tel1_dd && contato.email) {
                                    console.log(`[${new Date().toISOString()}] Found all needed contact info from company website. Skipping Google search.`);
                                    needGoogleSearch = false;
                                } else {
                                    console.log(`[${new Date().toISOString()}] Missing some contact info after scraping website. Will try Google search as well.`);
                                }
                            } else {
                                console.log(`[${new Date().toISOString()}] Site has very little content (${siteText.length} chars). Will use Google search instead.`);
                            }
                        } catch (siteError) {
                            console.error(`[${new Date().toISOString()}] Error accessing company site: ${bd1.site}`, siteError.message);
                            console.log(`[${new Date().toISOString()}] Will use Google search instead.`);
                            // Não marca como erro, apenas tenta via Google
                        }
                    }
                }
                
                // Se ainda precisamos buscar no Google (site não existente, erro ao acessar, ou faltam dados)
                if (needGoogleSearch) {
                    try {
                        // Busca informações adicionais no segundo banco de dados
                        const result2 = await pool2.query(`
                        SELECT 
                        c.trade_name, cr."name", c.address_fu, c.address_city_name, c.cnae_main 
                        FROM 
                        rf_company c
                        LEFT JOIN rf_company_root cr ON c.cnpj_root = cr.cnpj_root
                        LEFT JOIN rf_company_root_simples crs ON c.cnpj_root = crs.cnpj_root 
                        WHERE 
                        c.cnpj = '${bd1.cnpj}'
                        `);

                        if (result2.rows.length === 0) {
                            console.log(`[${new Date().toISOString()}] No additional info found for CNPJ: ${bd1.cnpj}`);
                            hasProcessingError = true; // Marca como erro pois faltam informações essenciais
                            continue;
                        }

                        const bd2 = result2.rows[0];  
                        
                        console.log("CNAE MAIn")
                        console.log(bd2.cnae_main)

                        // Monta a consulta para o Google com base nas informações da empresa
                        const empresaQuery = `${bd2.trade_name || bd2.name} ${bd2.address_city_name || ''} ${bd2.address_fu || ''}`;
                        const query = `${empresaQuery} ${bd1.cnpj} contato telefone email site`;
                        const searchUrl = `https://www.google.com/search?q=${encodeURIComponent(query)}`;

                        console.log(`[${new Date().toISOString()}] Searching Google for: ${query}`);

                        try {
                            // Add a delay before making the request to Google
                            console.log(`[${new Date().toISOString()}] Waiting 3 seconds before Google request...`);
                            await sleep(3000);
                            
                            // Cria uma instância do axios com um proxy aleatório para cada requisição ao Google
                            // Isso substitui a instância global do axios que você estava usando antes
                            const axiosInstance = await createAxiosWithProxy();
                            
                            // Faz a requisição HTTP para o Google usando o proxy
                            const response = await axiosInstance.get(searchUrl);
                            const html = response.data;
                            
                            // Verifica se o Google apresentou um CAPTCHA
                            const isCaptchaPresent = 
                                response.request.path.includes('/sorry/') ||
                                response.request.path.includes('captcha') ||
                                html.includes('unusual traffic') ||
                                html.includes('verify you') ||
                                html.includes('confirm you') ||
                                html.includes('not a robot') ||
                                html.includes('recaptcha');

                            if (isCaptchaPresent) {
                                console.error(`[${new Date().toISOString()}] CAPTCHA DETECTED! Attempt #${captchaCounter + 1}`);
                                captchaCounter++;
                                
                                if (captchaCounter >= 3) {
                                    console.log(`[${new Date().toISOString()}] CAPTCHA detected 3 times in a row. Waiting 3 minutes...`);
                                    // Espera 3 minutos antes de tentar novamente
                                    const captchaWaitTime = 3 * 60 * 1000; // 3 minutos em milissegundos
                                    await sleep(captchaWaitTime);
                                    captchaCounter = 0; // Reset counter after waiting
                                }
                                
                                // Troca o User-Agent para a próxima tentativa
                                axiosInstance.defaults.headers['User-Agent'] = randomUseragent.getRandom();
                                console.log(`[${new Date().toISOString()}] Changed User-Agent for next attempt`);
                                
                                // Aguarda um tempo antes de continuar
                                await sleep(10000);
                                continue;
                            }

                            // Carrega o HTML com cheerio
                            const $ = cheerio.load(html);

                            // Define expressões regulares para encontrar telefone, email e website
                            const regexTelefone = /(\(?\d{2}\)?\s*\d{4,5}[-.\s]?\d{4})/g;
                            const regexEmail = /([a-zA-Z0-9._-]+@[a-zA-Z0-9._-]+\.[a-zA-Z0-9._-]+)/g;
                            const regexSite = /(https?:\/\/(?:www\.)?[a-zA-Z0-9.-]+\.[a-zA-Z]{2,6})/g;

                            // Procurar por links de sites da empresa nos resultados do Google
                            const companyLinks = [];
                            
                            // Prepara termos para comparação
                            const companyName = (bd2.name || bd2.trade_name || '').toLowerCase();
                            // Remove caracteres especiais e divide em palavras
                            const companyWords = companyName
                                .replace(/[^\w\s]/gi, '')
                                .split(/\s+/)
                                .filter(word => word.length > 2); // Ignora palavras muito curtas

                            // Substituir a verificação de sites com esta lógica mais restritiva

                            // Extrai todos os links dos resultados de pesquisa do Google
                            $('a').each((i, link) => {
                                const href = $(link).attr('href');
                                
                                // Verifica se é um link externo (não do google)
                                if (href && href.startsWith('/url?q=')) {
                                    // Extrair a URL real do parâmetro q=
                                    let realUrl = href.substring(7);
                                    const endIndex = realUrl.indexOf('&');
                                    
                                    if (endIndex !== -1) {
                                        realUrl = realUrl.substring(0, endIndex);
                                    }
                                    
                                    // Ignora URLs do Google, YouTube, Facebook, etc. e gstatic
                                    const ignoreList = [
                                        'google.com', 
                                        'gstatic.com',
                                        'googleusercontent.com',
                                        'youtube.com', 
                                        'facebook.com', 
                                        'linkedin.com', 
                                        'instagram.com',
                                        'twitter.com',
                                        'wikipedia.org',
                                        'blogspot.com',
                                        'wordpress.com',
                                        'gov.br',
                                        'jus.br',
                                        'org.br',
                                        'netdna-ssl.com',
                                        'shopify.com',
                                        'amazonaws.com',
                                        'cloudfront.net',
                                        'cloudflare.com',
                                        'gov.br'
                                    ];
                                    const shouldIgnore = ignoreList.some(ignoreDomain => realUrl.includes(ignoreDomain));
                                    
                                    if (!shouldIgnore) {
                                        try {
                                            // Extrai o domínio para verificar se está relacionado à empresa
                                            const url = new URL(realUrl);
                                            const domain = url.hostname.toLowerCase();
                                            
                                            // Limpa o domínio para comparação
                                            const cleanDomain = domain.replace('www.', '');
                                            
                                            // Verifica se o domínio contém partes significativas do nome da empresa
                                            let isRelated = false;
                                            
                                            // Prepara o nome da empresa simplificado (remove LTDA, ME, etc.)
                                            const simplifiedCompanyName = companyName
                                                .replace(/\bltda\b|\bme\b|\bepp\b|\bsa\b|\beireli\b|\bcompany\b|\binc\b|\bcorp\b/g, '')
                                                .trim();
                                            
                                            // Prepara uma versão sem espaços para correspondência exata
                                            const noSpaceCompanyName = simplifiedCompanyName.replace(/\s+/g, '');
                                            
                                            // Separa o domínio principal (antes do primeiro ponto)
                                            const mainDomainPart = cleanDomain.split('.')[0];
                                            
                                            // VERIFICAÇÃO 1: Correspondência direta entre nome da empresa e domínio principal
                                            // Esta é a verificação mais restritiva e confiável
                                            if (mainDomainPart === noSpaceCompanyName || 
                                                (noSpaceCompanyName.length > 5 && mainDomainPart.includes(noSpaceCompanyName)) || 
                                                (mainDomainPart.length > 5 && noSpaceCompanyName.includes(mainDomainPart))) {
                                                isRelated = true;
                                                console.log(`[${new Date().toISOString()}] Found exact company match: ${domain} matches "${noSpaceCompanyName}"`);
                                            }
                                            
                                            // VERIFICAÇÃO 2: Correspondência de palavras significativas
                                            // Se a verificação 1 falhar, procuramos palavras significativas do nome da empresa no domínio
                                            if (!isRelated) {
                                                // Filtra apenas palavras significativas (mais de 3 caracteres e não genéricas)
                                                const significantWords = companyWords.filter(word => 
                                                    word.length > 3 && 
                                                    !['para', 'com', 'dos', 'das', 'ltda', 'epp', 'eireli'].includes(word)
                                                );
                                                
                                                for (const word of significantWords) {
                                                    // A palavra precisa ser uma parte substancial do domínio
                                                    if (word.length > 4 && mainDomainPart.includes(word)) {
                                                        isRelated = true;
                                                        console.log(`[${new Date().toISOString()}] Found significant word match: ${domain} contains "${word}"`);
                                                        break;
                                                    }
                                                }
                                            }
                                            
                                            // VERIFICAÇÃO 3: Verificação por combinação de palavras (apenas para nomes compostos)
                                            // Útil para casos como "Top Clima" que se torna "topclima" no domínio
                                            if (!isRelated && companyWords.length >= 2) {
                                                // Apenas verificamos combinações de palavras adjacentes
                                                for (let i = 0; i < companyWords.length - 1; i++) {
                                                    if (companyWords[i].length > 2 && companyWords[i+1].length > 2) {
                                                        const combinedWord = companyWords[i] + companyWords[i + 1];
                                                        if (combinedWord.length > 5 && mainDomainPart === combinedWord) {
                                                            isRelated = true;
                                                            console.log(`[${new Date().toISOString()}] Found exact combined word match: ${domain} equals "${combinedWord}"`);
                                                            break;
                                                        }
                                                    }
                                                }
                                            }
                                            
                                            // VERIFICAÇÃO 4: Verificação por lista de domínios conhecidos
                                            // Para casos especiais que sabemos que são válidos
                                            if (!isRelated) {
                                                const knownDomains = [
                                                    { company: "topclima", domain: "topclima.com.br" },
                                                    // adicione outros domínios conhecidos conforme necessário
                                                ];
                                                
                                                for (const known of knownDomains) {
                                                    if (companyName.includes(known.company) && domain === known.domain) {
                                                        isRelated = true;
                                                        console.log(`[${new Date().toISOString()}] Found domain from known list: ${domain}`);
                                                        break;
                                                    }
                                                }
                                            }
                                            
                                            // Verificação adicional: checa se o texto do link indica que é o site oficial
                                            if (!isRelated) {
                                                const linkText = $(link).text().toLowerCase();
                                                // Corrigido para usar linkText.includes em todas as verificações
                                                if (linkText.includes('site oficial') || 
                                                    linkText.includes('website') || 
                                                    linkText.includes('página oficial') || 
                                                    linkText.includes('oficial')) {
                                                    
                                                    // Mesmo para links marcados como "site oficial", ainda verificamos alguma relação com o nome
                                                    const hasAnyRelation = companyWords.some(word => 
                                                        word.length > 3 && mainDomainPart.includes(word)
                                                    );
                                                    
                                                    if (hasAnyRelation) {
                                                        isRelated = true;
                                                        console.log(`[${new Date().toISOString()}] Found likely official site: ${domain} (link text suggests official site)`);
                                                    } else {
                                                        console.log(`[${new Date().toISOString()}] Ignoring potential official site with no name relation: ${domain}`);
                                                    }
                                                }
                                            }
                                            
                                            if (isRelated) {
                                                companyLinks.push(realUrl);
                                            }
                                        } catch (e) {
                                            // Ignora URLs inválidas
                                            console.log(`[${new Date().toISOString()}] Error processing URL: ${e.message}`);
                                        }
                                    }
                                }
                            });

                            // Se não encontrou links da empresa, tenta buscar nos resultados especiais do Google
                            if (companyLinks.length === 0) {
                                console.log(`[${new Date().toISOString()}] No company links found. Checking for business info cards...`);
                                
                                // Procura nos painéis de informação da empresa (Google Business Profile)
                                $('.kp-header a').each((i, link) => {
                                    const href = $(link).attr('href');
                                    if (href && !href.startsWith('/')) {
                                        try {
                                            const url = new URL(href);
                                            console.log(`[${new Date().toISOString()}] Found website from business profile: ${href}`);
                                            companyLinks.push(href);
                                        } catch (e) {
                                            // Ignora URLs inválidas
                                        }
                                    }
                                });
                                
                                // Procura no painel lateral de informações
                                $('.Z1hOCe a[href], .zloOqf a[href]').each((i, link) => {
                                    const href = $(link).attr('href');
                                    if (href && !href.startsWith('/') && !href.includes('google.com')) {
                                        try {
                                            console.log(`[${new Date().toISOString()}] Found website from info panel: ${href}`);
                                            
                                            console.log(`[${new Date().toISOString()}] Site does not exist or is not reachable: ${siteUrl}`);
                                            
                                            // Se o site não existir, remova-o da lista de sites da empresa
                                            const index = companyLinks.indexOf(siteUrl);
                                            if (index > -1) {
                                                companyLinks.splice(index, 1);
                                            }
                                            
                                            // Não use esse site como fonte de dados
                                            if (contato.site === siteUrl) {
                                                contato.site = null;
                                            }
                                        } catch (e) {
                                            // Ignora URLs inválidas
                                            console.log(`[${new Date().toISOString()}] Error processing URL: ${e.message}`);
                                        }
                                    }
                                });
                                
                                // IMPORTANTE: Se visitou sites da empresa, não deve buscar dados do Google
                                // A menos que não encontrou todos os dados necessários
                                if (!contato.telefone || !contato.email) {
                                    console.log(`[${new Date().toISOString()}] Missing some contact data after checking company sites. Supplementing with Google results.`);
                                    
                                    // Extrai todo o texto da página do Google para complementar os dados faltantes
                                    const pageText = $('body').text();
                                    
                                    // Apenas procura o telefone se ainda não o encontrou nos sites da empresa
                                    if (!contato.telefone) {
                                        const infoTelefone = extrairTelefone(pageText);
                                        if (infoTelefone) {
                                            contato.telefone = infoTelefone.telefoneCompleto;
                                            contato.tel1_dd = infoTelefone.tel1_dd;
                                            contato.tel1 = infoTelefone.tel1;
                                            console.log(`[${new Date().toISOString()}] Telefone complementado da página do Google: ${contato.telefone}, DDD: ${contato.tel1_dd}, Número: ${contato.tel1}`);
                                        }
                                    }
                                    
                                    // Apenas procura o email se ainda não o encontrou nos sites da empresa
                                    if (!contato.email) {
                                        const emailEncontrado = extrairEmail(pageText);
                                        if (emailEncontrado && !isBlacklistedEmail(emailEncontrado)) {
                                            contato.email = emailEncontrado;
                                            console.log(`[${new Date().toISOString()}] Email complementado da página do Google: ${contato.email}`);
                                        } else if (emailEncontrado) {
                                            console.log(`[${new Date().toISOString()}] Email encontrado em lista negra, ignorado: ${emailEncontrado}`);
                                        }
                                    }
                                }
                            } else {
                                // Processa dados do Google quando nenhum site foi encontrado
                                console.log(`[${new Date().toISOString()}] No company-specific sites found. Using Google results page.`);
                                
                                // Extrai todo o texto da página do Google
                                const pageText = $('body').text();
                                
                                // Procura telefone nos resultados do Google
                                const infoTelefone = extrairTelefone(pageText);
                                if (infoTelefone) {
                                    contato.telefone = infoTelefone.telefoneCompleto;
                                    contato.tel1_dd = infoTelefone.tel1_dd;
                                    contato.tel1 = infoTelefone.tel1;
                                    console.log(`[${new Date().toISOString()}] Telefone encontrado nos resultados do Google: ${contato.telefone}, DDD: ${contato.tel1_dd}, Número: ${contato.tel1}`);
                                }
                                
                                // Procura email nos resultados do Google
                                const emailEncontrado = extrairEmail(pageText);
                                if (emailEncontrado && !isBlacklistedEmail(emailEncontrado)) {
                                    contato.email = emailEncontrado;
                                    console.log(`[${new Date().toISOString()}] Email encontrado nos resultados do Google: ${contato.email}`);
                                } else if (emailEncontrado) {
                                    console.log(`[${new Date().toISOString()}] Email encontrado em lista negra, ignorado: ${emailEncontrado}`);
                                }
                                
                                // Procura site nos resultados do Google
                                const matchSite = pageText.match(regexSite);
                                if (matchSite && matchSite.length) {
                                    // Filtra para evitar sites conhecidos não relacionados à empresa
                                    const ignoreList = [
                                        'google.com', 'youtube.com', 'facebook.com', 'linkedin.com', 
                                        'instagram.com', 'twitter.com', 'wikipedia.org'
                                    ];
                                    
                                    for (const potentialSite of matchSite) {
                                        const shouldIgnore = ignoreList.some(ignoreSite => 
                                            potentialSite.includes(ignoreSite)
                                        );
                                        
                                        if (!shouldIgnore) {
                                            contato.site = potentialSite;
                                            console.log(`[${new Date().toISOString()}] Site potencial encontrado nos resultados do Google: ${contato.site}`);
                                            break;
                                        }
                                    }
                                }
                                
                                // Procura especificamente por blocos de informação de contato no Google
                                $('.kp-header, .Z1hOCe, .zloOqf, .ruhjFe').each((_, element) => {
                                    const infoBlockText = $(element).text();
                                    
                                    // Se ainda não temos telefone, tenta extrair deste bloco
                                    if (!contato.telefone) {
                                        const blockTelefone = extrairTelefone(infoBlockText);
                                        if (blockTelefone) {
                                            contato.telefone = blockTelefone.telefoneCompleto;
                                            contato.tel1_dd = blockTelefone.tel1_dd;
                                            contato.tel1 = blockTelefone.tel1;
                                            console.log(`[${new Date().toISOString()}] Telefone encontrado em bloco de informação do Google: ${contato.telefone}`);
                                        }
                                    }
                                    
                                    // Se ainda não temos email, tenta extrair deste bloco
                                    if (!contato.email) {
                                        const blockEmail = extrairEmail(infoBlockText);
                                        if (blockEmail && !isBlacklistedEmail(blockEmail)) {
                                            contato.email = blockEmail;
                                            console.log(`[${new Date().toISOString()}] Email encontrado em bloco de informação do Google: ${contato.email}`);
                                        } else if (blockEmail) {
                                            console.log(`[${new Date().toISOString()}] Email encontrado em lista negra, ignorado: ${blockEmail}`);
                                        }
                                    }
                                });
                            }
                        } catch (error) {
                            console.error(`[${new Date().toISOString()}] Error fetching search results:`, error.message);
                            
                            // Tratamento específico para erro 429 (Too Many Requests)
                            if (error.response?.status === 429) {
                                console.error(`[${new Date().toISOString()}] RATE LIMIT DETECTED (429)! Google is blocking our requests.`);
                                
                                // Implementa um backoff exponencial - quanto mais receber 429, mais tempo espera
                                const backoffTime = Math.pow(2, captchaCounter + 2) * 10000; // 40s, 80s, 160s, etc.
                                console.log(`[${new Date().toISOString()}] Waiting ${backoffTime/1000} seconds before retrying...`);
                                
                                // Incrementa o contador de captcha (reutilizando para o backoff)
                                captchaCounter++;
                                
                                // Troca o User-Agent para próxima tentativa
                                axiosInstance.defaults.headers['User-Agent'] = randomUseragent.getRandom();
                                console.log(`[${new Date().toISOString()}] Changed User-Agent to: ${axiosInstance.defaults.headers['User-Agent']}`);
                                
                                // Espera o tempo de backoff antes de continuar
                                await sleep(backoffTime);
                                continue; // Continua com o mesmo registro após a espera
                            }
                            
                            // Tratamento para outros erros (existente)
                            if (error.code === 'ECONNABORTED' || error.response?.status === 403) {
                                console.log(`[${new Date().toISOString()}] Connection issue or access denied. Waiting 30 seconds...`);
                                await sleep(30000);
                                // Troca o User-Agent para a próxima tentativa
                                axiosInstance.defaults.headers['User-Agent'] = randomUseragent.getRandom();
                            }
                            hasProcessingError = true; // Marca como erro
                            continue; // Pula para o próximo registro
                        }

                        // Se não houve erro, tenta salvar os dados no banco
                        if (!hasProcessingError) {
                            try {
                                console.log(`[${new Date().toISOString()}] Contact data found:`, contato);

                                // Cria o array de campos e valores para atualização
                                const fieldsToUpdate = [];
                                const valuesToUpdate = [];
                                let paramIndex = 1;

                                // Adiciona campos somente se tiverem valores
                                if (contato.tel1_dd) {
                                    fieldsToUpdate.push(`tel1_dd = $${paramIndex}`);
                                    valuesToUpdate.push(contato.tel1_dd);
                                    paramIndex++;
                                }

                                if (contato.tel1) {
                                    fieldsToUpdate.push(`tel1 = $${paramIndex}`);
                                    valuesToUpdate.push(contato.tel1);
                                    paramIndex++;
                                }

                                // Verifica se o email não está na lista negra antes de incluí-lo na atualização
                                if (contato.email && !isBlacklistedEmail(contato.email)) {
                                    fieldsToUpdate.push(`email = $${paramIndex}`);
                                    valuesToUpdate.push(contato.email);
                                    paramIndex++;
                                } else if (contato.email) {
                                    console.log(`[${new Date().toISOString()}] Email em lista negra não será salvo: ${contato.email}`);
                                    contato.email = null; // Define como null para não usar email na lista negra
                                }

                                if (contato.site) {
                                    fieldsToUpdate.push(`site = $${paramIndex}`);
                                    valuesToUpdate.push(contato.site);
                                    paramIndex++;
                                }

                                // Sempre adiciona update_google = 1 e at = 2
                                fieldsToUpdate.push(`update_google = 1`);
                                fieldsToUpdate.push(`at = 2`);
                                
                                // Adiciona o ID como último parâmetro
                                valuesToUpdate.push(bd1.id);

                                // Atualiza o banco de dados somente se houver campos para atualizar
                                if (fieldsToUpdate.length > 0) {
                                    const updateQuery = `
                                    UPDATE transfer
                                    SET ${fieldsToUpdate.join(', ')}
                                    WHERE id = $${paramIndex}
                                    `;

                                    try {
                                        await pool1.query(updateQuery, valuesToUpdate);
                                        console.log(`[${new Date().toISOString()}] Updated record ID: ${bd1.id}`);
                                    } catch (error) {
                                        console.error(`[${new Date().toISOString()}] Error updating record ID ${bd1.id}:`, error.message);
                                        // Não incrementa o contador de registros processados em caso de erro no banco
                                        continue;
                                    }
                                }

                                recordsProcessed++;
                                console.log(`[${new Date().toISOString()}] Records processed: ${recordsProcessed}`);

                            } catch (error) {
                                console.error(`[${new Date().toISOString()}] Error during data processing:`, error);
                                // Erro na formatação/processamento dos dados, pula para o próximo registro
                                continue;
                            }
                        } else {
                            console.log(`[${new Date().toISOString()}] Skipping database update due to processing errors for CNPJ: ${bd1.cnpj}`);
                        }
                    } catch (error) {
                        console.error(`[${new Date().toISOString()}] Error querying database for company info:`, error.message);
                        hasProcessingError = true; // Marca como erro
                        continue; // Pula para o próximo registro
                    }
                }
            }

            console.log(`[${new Date().toISOString()}] Script completed. Total records processed: ${recordsProcessed}`);

        } catch (error) {
            console.error(`[${new Date().toISOString()}] Error during scraping:`, error);
        } finally {
            // Only close the database connections once, at the end of all processing
            try {
                // Check if pools are still active before closing
                if (pool1) {
                    await pool1.end();
                }
                if (pool2) {
                    await pool2.end();
                }
                console.log(`[${new Date().toISOString()}] Database connections closed.`);
            } catch (closeError) {
                console.error(`[${new Date().toISOString()}] Error closing database connections:`, closeError.message);
            }
        }
    }
    
    await startScraping();
})();