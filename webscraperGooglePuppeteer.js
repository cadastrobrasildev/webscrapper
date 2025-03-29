// Instale as dependências:
// npm install puppeteer cheerio random-useragent pg dotenv

const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const cheerio = require('cheerio');
const randomUseragent = require('random-useragent');
const { Pool } = require('pg');
const sleep = ms => new Promise(res => setTimeout(res, ms));
require('dotenv').config();
puppeteer.use(StealthPlugin());
const os = require('os');
const proxyAgent = require('proxy-agent');
const axios = require('axios');
const ProxyChain = require('proxy-chain');

// Sistema de gerenciamento de proxies com proxy-chain
let proxyPool = [];
let currentProxy = null;
let proxyUrl = null;
let proxyServer = null;
let proxyRequestCount = 0; // Add this variable declaration
const MAX_REQUESTS_PER_PROXY = 10; // Reduzido para maior segurança
const MIN_PROXIES_IN_POOL = 5;

/**
 * Inicializa a pool de proxies usando proxy-chain e serviços premium gratuitos
 */
async function initializeProxyPool() {
    console.log(`[${new Date().toISOString()}] Iniciando coleta de proxies...`);
    proxyPool = [];
    
    try {
        // 1. Primeiro tenta proxies premium gratuitos (ProxyScrape)
        const response = await axios.get('https://api.proxyscrape.com/v2/?request=getproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=elite', {
            timeout: 10000
        });
        
        if (response.status === 200 && response.data) {
            const lines = response.data.split('\n').filter(line => line.trim() && line.includes(':'));
            
            for (const line of lines) {
                const [ip, port] = line.trim().split(':');
                if (ip && port) {
                    proxyPool.push({
                        ip: ip.trim(),
                        port: parseInt(port.trim(), 10),
                        protocol: 'http',
                        lastUsed: null,
                        working: true
                    });
                }
            }
            
            console.log(`[${new Date().toISOString()}] ProxyScrape: Encontrados ${proxyPool.length} proxies`);
        }
    } catch (error) {
        console.error(`[${new Date().toISOString()}] Erro ao obter proxies premium:`, error.message);
    }
    
    // 2. Se não encontrou proxies suficientes, adiciona os estáticos confiáveis
    if (proxyPool.length < MIN_PROXIES_IN_POOL) {
        await addStaticProxies();
    }
    
    // 3. Se mesmo assim não tem proxies suficientes, usa o ProxyMesh (serviço gratuito com limite)
    if (proxyPool.length < MIN_PROXIES_IN_POOL) {
        try {
            proxyPool.push({
                ip: 'open.proxymesh.com',
                port: 31280,
                protocol: 'http',
                lastUsed: null,
                working: true
            });
            console.log(`[${new Date().toISOString()}] Adicionado proxy ProxyMesh`);
        } catch (error) {
            console.error(`[${new Date().toISOString()}] Erro ao adicionar ProxyMesh:`, error.message);
        }
    }
    
    // 4. Por fim, adiciona conexão direta como último recurso
    proxyPool.push({
        ip: null,
        port: null,
        country: 'local',
        protocol: 'direct',
        lastUsed: null,
        working: true
    });
    
    console.log(`[${new Date().toISOString()}] Pool de proxies inicializada com ${proxyPool.length} proxies`);
    
    // Inicia o proxy server local usando proxy-chain (se necessário)
    await initializeProxyServer();
}

/**
 * Adiciona proxies estáticos confiáveis à pool
 */
async function addStaticProxies() {
    const staticProxies = [
        { ip: '200.25.254.193', port: 54240, country: 'br', protocol: 'http' },
        { ip: '177.93.38.74', port: 999, country: 'co', protocol: 'http' },
        { ip: '47.74.152.29', port: 8888, country: 'us', protocol: 'http' },
        { ip: '51.79.52.80', port: 3080, country: 'ca', protocol: 'http' },
        { ip: '167.99.147.121', port: 3128, country: 'ca', protocol: 'http' },
        { ip: '190.196.176.5', port: 60080, country: 'ar', protocol: 'http' },
        { ip: '146.190.83.209', port: 3128, country: 'us', protocol: 'http' },
        { ip: '45.232.79.1', port: 9292, country: 'br', protocol: 'http' },
        { ip: '190.61.88.147', port: 8080, country: 'co', protocol: 'http' },
        { ip: '190.90.8.74', port: 8080, country: 'co', protocol: 'http' },
        { ip: '147.182.132.21', port: 3128, country: 'us', protocol: 'http' }
    ];
    
    for (const proxy of staticProxies) {
        proxy.lastUsed = null;
        proxy.working = true;
        proxyPool.push(proxy);
    }
    
    console.log(`[${new Date().toISOString()}] Adicionados ${staticProxies.length} proxies estáticos`);
}

/**
 * Inicializa o servidor de proxy local com proxy-chain
 */
async function initializeProxyServer() {
    // Encerra servidor existente se houver
    if (proxyServer) {
        try {
            await proxyServer.close();
            console.log(`[${new Date().toISOString()}] Servidor de proxy existente fechado`);
        } catch (error) {
            console.error(`[${new Date().toISOString()}] Erro ao fechar servidor de proxy:`, error.message);
        }
    }
    
    try {
        // Cria um novo servidor de proxy local
        proxyServer = new ProxyChain.Server({
            // Esta função é chamada quando há uma solicitação para o proxy
            prepareRequestFunction: async ({ request, username, password, hostname, port, isHttp }) => {
                // Pega um proxy aleatório da pool para rotacionar IPs
                const randomProxy = await getRandomProxy();
                
                if (randomProxy.protocol === 'direct') {
                    // Conecta diretamente sem proxy
                    console.log(`[${new Date().toISOString()}] Usando conexão direta para ${hostname}:${port}`);
                    return {
                        requestAuthentication: false,
                        upstreamProxyUrl: null,
                    };
                } else {
                    // Usa o proxy selecionado
                    const upstreamProxyUrl = `http://${randomProxy.ip}:${randomProxy.port}`;
                    console.log(`[${new Date().toISOString()}] Usando proxy para ${hostname}:${port}: ${upstreamProxyUrl}`);
                    return {
                        requestAuthentication: false,
                        upstreamProxyUrl,
                    };
                }
            },
        });
        
        // Inicia o servidor de proxy
        await proxyServer.listen(0); // Porta aleatória
        const port = proxyServer.port;
        proxyUrl = `http://localhost:${port}`;
        console.log(`[${new Date().toISOString()}] Servidor de proxy local iniciado em ${proxyUrl}`);
    } catch (error) {
        console.error(`[${new Date().toISOString()}] Erro ao iniciar servidor de proxy:`, error.message);
        proxyServer = null;
        proxyUrl = null;
    }
}

/**
 * Obtém um proxy aleatório da pool
 */
async function getRandomProxy() {
    if (proxyPool.length === 0) {
        await initializeProxyPool();
    }
    
    // Tenta até 3 proxies aleatórios
    for (let attempt = 0; attempt < 3; attempt++) {
        // Primeiro tenta proxies não usados
        const unusedProxies = proxyPool.filter(p => p.lastUsed === null && p.working);
        
        let proxy;
        if (unusedProxies.length > 0) {
            proxy = unusedProxies[Math.floor(Math.random() * unusedProxies.length)];
        } else {
            // Se todos já foram usados, pega qualquer um marcado como funcionando
            const workingProxies = proxyPool.filter(p => p.working);
            if (workingProxies.length > 0) {
                proxy = workingProxies[Math.floor(Math.random() * workingProxies.length)];
            } else {
                // Se nenhum funciona, usa conexão direta
                return {
                    ip: null,
                    port: null,
                    protocol: 'direct',
                    lastUsed: new Date(),
                    working: true
                };
            }
        }
        
        // Testa o proxy antes de retornar
        try {
            proxy.working = await testProxy(proxy);
            
            if (proxy.working) {
                proxy.lastUsed = new Date();
                console.log(`[${new Date().toISOString()}] Proxy selecionado: ${proxy.ip}:${proxy.port}`);
                return proxy;
            } else {
                console.log(`[${new Date().toISOString()}] Proxy falhou no teste: ${proxy.ip}:${proxy.port}`);
            }
        } catch (error) {
            console.error(`[${new Date().toISOString()}] Erro testando proxy ${proxy.ip}:${proxy.port}:`, error.message);
            proxy.working = false;
        }
    }
    
    // Se todos falharem, usa conexão direta
    console.log(`[${new Date().toISOString()}] Todos os proxies falharam, usando conexão direta`);
    return {
        ip: null,
        port: null,
        protocol: 'direct',
        lastUsed: new Date(),
        working: true
    };
}

/**
 * Testa se um proxy está funcionando
 */
async function testProxy(proxy) {
    if (proxy.protocol === 'direct') return true;
    
    try {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 5000);
        
        const testResponse = await axios.get('https://api.ipify.org?format=json', {
            proxy: {
                host: proxy.ip,
                port: proxy.port,
                protocol: proxy.protocol
            },
            timeout: 5000,
            signal: controller.signal
        });
        
        clearTimeout(timeoutId);
        return testResponse.status === 200;
    } catch (error) {
        return false;
    }
}

const getLocalIP = () => {
    const interfaces = os.networkInterfaces();
    for (const iface of Object.values(interfaces)) {
        for (const config of iface) {
            if (config.family === 'IPv4' && !config.internal) {
                return config.address;
            }
        }
    }
    return 'IP não encontrado';
};

console.log(`IP do Servidor: ${getLocalIP()}`);

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
console.log("v5 - Com rotação de proxies")
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
    // Expressão regular para encontrar e-mails
    const regexEmail = /([a-zA-Z0-9._-]+@[a-zA-Z0-9._-]+\.[a-zA-Z0-9._-]+)/g;

    // Encontra todos os e-mails no texto
    const emails = texto.match(regexEmail);

    if (!emails || emails.length === 0) {
        console.log(`[${new Date().toISOString()}] Nenhum e-mail encontrado no texto com regex padrão`);

        // Tenta encontrar emails ofuscados (por exemplo, "contato (at) dominio (dot) com")
        const regexOfuscado = /([a-zA-Z0-9._-]+)[\s]*[\[\(]?at[\]\)]?[\s]*([a-zA-Z0-9._-]+)[\s]*[\[\(]?dot[\]\)]?[\s]*([a-zA-Z0-9._-]+)/gi;
        const ofuscados = texto.match(regexOfuscado);

        if (ofuscados && ofuscados.length > 0) {
            // Converte o primeiro email ofuscado para formato normal
            const partes = ofuscados[0].match(/([a-zA-Z0-9._-]+)[\s]*[\[\(]?at[\]\)]?[\s]*([a-zA-Z0-9._-]+)[\s]*[\[\(]?dot[\]\)]?[\s]*([a-zA-Z0-9._-]+)/i);
            if (partes && partes.length >= 4) {
                const emailReconstruido = `${partes[1].trim()}@${partes[2].trim()}.${partes[3].trim()}`;
                console.log(`[${new Date().toISOString()}] E-mail ofuscado reconstruído: ${emailReconstruido}`);
                return emailReconstruido;
            }
        }

        return null;
    }

    // Itera pelos e-mails encontrados para escolher o mais relevante
    for (const email of emails) {
        console.log(`[${new Date().toISOString()}] E-mail encontrado (bruto): ${email}`);

        // Verifica se o e-mail tem formato válido
        if (email.indexOf('@') > 0 && email.indexOf('.') > 0) {
            // Ignora e-mails genéricos ou de serviços conhecidos
            const dominiosGenericos = [
                'gmail.com', 'hotmail.com', 'outlook.com', 'yahoo.com',
                'live.com', 'icloud.com', 'aol.com', 'mail.com',
                'protonmail.com', 'yandex.com', 'zoho.com'
            ];

            const dominio = email.split('@')[1].toLowerCase();

            // Se for um domínio conhecido de serviço de e-mail, continua procurando
            if (dominiosGenericos.includes(dominio)) {
                console.log(`[${new Date().toISOString()}] E-mail de serviço comum: ${email} - continuando busca`);
                continue;
            }

            // Verifica se o e-mail não parece ser de newsletter, suporte, etc.
            const emailsGenericos = [
                'info@', 'contato@', 'contact@', 'mail@', 'email@',
                'support@', 'suporte@', 'noreply@', 'no-reply@',
                'newsletter@', 'news@', 'marketing@', 'webmaster@',
                'admin@', 'administrador@', 'administrator@',
                'help@', 'ajuda@', 'sac@'
            ];

            // Se for um e-mail genérico, continue procurando
            if (emailsGenericos.some(prefix => email.toLowerCase().startsWith(prefix))) {
                console.log(`[${new Date().toISOString()}] E-mail genérico: ${email} - continuando busca`);
                continue;
            }

            console.log(`[${new Date().toISOString()}] E-mail válido encontrado: ${email}`);
            return email;
        }
    }

    // Se não encontrou um e-mail "bom", retorna o primeiro da lista (se houver)
    if (emails.length > 0) {
        console.log(`[${new Date().toISOString()}] Retornando primeiro e-mail da lista: ${emails[0]}`);
        return emails[0];
    }

    console.log(`[${new Date().toISOString()}] Nenhum e-mail válido encontrado`);
    return null;
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

const userDataDir = `./temp/profile_${Math.random().toString(36).substring(7)}`;

/**
 * Inicializa o navegador Puppeteer com configurações otimizadas
 * @returns {Promise<Browser>} Instância do navegador
 * 
 * 
 */

function getValidUserAgent() {
    try {
        const ua = randomUseragent.getRandom(ua => {
            return ua.browserName === 'Chrome' && 
                   parseFloat(ua.browserVersion) >= 90 &&
                   ua.osName === 'Windows';
        });
        
        // Verificação adicional para garantir que seja uma string válida
        if (typeof ua === 'string' && ua.length > 50 && ua.includes('Chrome')) {
            return ua;
        }
        // Fallback para um User-Agent válido
        return 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';
    } catch (e) {
        console.error('Erro ao gerar User-Agent:', e);
        return 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';
    }
}

// Modify the initBrowser function to better handle proxy issues

async function initBrowser() {
    try {
        // Seleciona um proxy se ainda não temos um
        if (!currentProxy) {
            currentProxy = await getRandomProxy();
        }
        
        console.log(`[${new Date().toISOString()}] Usando proxy: ${currentProxy.protocol === 'direct' ? 'conexão direta' : `${currentProxy.ip}:${currentProxy.port} (${currentProxy.country || 'unknown'})`}`);

        const args = [
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-infobars',
            '--disable-web-security',
            '--disable-features=IsolateOrigins,site-per-process',
            '--disable-blink-features=AutomationControlled',
            '--disable-dev-shm-usage',
            '--disable-accelerated-2d-canvas',
            '--no-first-run',
            '--no-zygote',
            '--window-size=1366,768' // Tamanho comum de janela
        ];

        // Adiciona o user agent
        args.push(`--user-agent=${getValidUserAgent()}`);
        
        // Adiciona o proxy apenas se não for conexão direta
        if (currentProxy.protocol !== 'direct' && currentProxy.ip && currentProxy.port) {
            args.push(`--proxy-server=${currentProxy.ip}:${currentProxy.port}`);
        }

        return await puppeteer.launch({
            headless: 'new',
            userDataDir: userDataDir,
            args: args,
            ignoreHTTPSErrors: true,
            defaultViewport: null
        });
    } catch (error) {
        console.error(`[${new Date().toISOString()}] Erro ao inicializar navegador:`, error.message);
        
        // Se falhar, tenta conexão direta
        console.log(`[${new Date().toISOString()}] Tentando inicializar com conexão direta após falha`);
        currentProxy = {
            ip: null,
            port: null,
            protocol: 'direct',
            lastUsed: new Date(),
            working: true
        };
        
        return await puppeteer.launch({
            headless: 'new',
            userDataDir: userDataDir,
            args: [
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-infobars',
                '--disable-web-security',
                '--disable-features=IsolateOrigins,site-per-process',
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--disable-accelerated-2d-canvas',
                '--no-first-run',
                '--no-zygote',
                '--window-size=1366,768',
                `--user-agent=${getValidUserAgent()}`
            ],
            ignoreHTTPSErrors: true,
            defaultViewport: null
        });
    }
}

/**
 * Rotaciona o proxy e reinicia o navegador
 * @param {Browser} browser - Instância atual do navegador
 * @returns {Promise<Browser>} Nova instância do navegador
 */
async function rotateProxyAndRestartBrowser(browser) {
    // Fecha o navegador atual se existir
    if (browser) {
        try {
            await browser.close();
            console.log(`[${new Date().toISOString()}] Navegador fechado para rotação de proxy`);
        } catch (error) {
            console.error(`[${new Date().toISOString()}] Erro ao fechar navegador para rotação:`, error.message);
            // Continue mesmo se falhar ao fechar (pode já estar fechado)
        }
    }
    
    // Pequena pausa antes de reabrir o navegador
    await sleep(2000); // Aumentado para 2 segundos para garantir que o navegador anterior seja fechado corretamente
    
    // Array para armazenar tentativas de proxy
    const proxyAttempts = [];
    let newBrowser = null;
    let attemptCount = 0;
    const maxAttempts = 3;
    
    // Tenta até conseguir um proxy que funcione ou esgotar o número máximo de tentativas
    while (attemptCount < maxAttempts && !newBrowser) {
        attemptCount++;
        
        try {
            console.log(`[${new Date().toISOString()}] Tentativa ${attemptCount} de ${maxAttempts} para encontrar proxy funcionando`);
            
            // Seleciona um novo proxy que ainda não foi tentado nesta sessão de rotação
            try {
                let foundNewProxy = false;
                for (let i = 0; i < 3; i++) { // Tenta até 3 vezes encontrar um proxy não utilizado recentemente
                    const candidateProxy = await getRandomProxy();
                    
                    // Verifica se este proxy já foi tentado nesta sessão
                    const alreadyTried = proxyAttempts.some(p => 
                        p.ip === candidateProxy.ip && p.port === candidateProxy.port
                    );
                    
                    if (!alreadyTried) {
                        currentProxy = candidateProxy;
                        foundNewProxy = true;
                        break;
                    }
                }
                
                // Se não conseguiu um novo proxy, usa conexão direta
                if (!foundNewProxy) {
                    console.log(`[${new Date().toISOString()}] Não foi possível encontrar um novo proxy. Usando conexão direta.`);
                    currentProxy = {
                        ip: null,
                        port: null,
                        protocol: 'direct',
                        lastUsed: new Date(),
                        working: true
                    };
                }
                
                proxyRequestCount = 0; // Reinicia contador de requisições
                
                // Registra o proxy tentado
                if (currentProxy.protocol !== 'direct') {
                    proxyAttempts.push({
                        ip: currentProxy.ip,
                        port: currentProxy.port,
                        protocol: currentProxy.protocol
                    });
                    
                    console.log(`[${new Date().toISOString()}] Rotacionando para novo proxy: ${currentProxy.ip}:${currentProxy.port} (${currentProxy.country || 'unknown'})`);
                } else {
                    console.log(`[${new Date().toISOString()}] Usando conexão direta (sem proxy)`);
                }
            } catch (error) {
                console.error(`[${new Date().toISOString()}] Erro ao selecionar novo proxy:`, error.message);
                // Em caso de erro, usa conexão direta
                currentProxy = {
                    ip: null,
                    port: null,
                    protocol: 'direct',
                    lastUsed: new Date(),
                    working: true
                };
                console.log(`[${new Date().toISOString()}] Usando conexão direta devido a erro na seleção de proxy`);
            }
            
            // Testa o proxy antes de inicializar o navegador (apenas para proxies não diretos)
            if (currentProxy.protocol !== 'direct') {
                const proxyWorks = await testProxy(currentProxy);
                if (!proxyWorks) {
                    console.log(`[${new Date().toISOString()}] Proxy falhou no teste prévio. Tentando outro...`);
                    continue; // Tenta o próximo proxy
                }
                console.log(`[${new Date().toISOString()}] Proxy passou no teste prévio.`);
            }
            
            // Inicializa um novo navegador com o novo proxy
            console.log(`[${new Date().toISOString()}] Iniciando novo navegador após rotação de proxy...`);
            
            const args = [
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-infobars',
                '--disable-web-security',
                '--disable-features=IsolateOrigins,site-per-process',
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--disable-accelerated-2d-canvas',
                '--no-first-run',
                '--no-zygote',
                '--window-size=1366,768',
                `--user-agent=${getValidUserAgent()}`
            ];
            
            // Adiciona o proxy apenas se não for conexão direta
            if (currentProxy.protocol !== 'direct' && currentProxy.ip && currentProxy.port) {
                args.push(`--proxy-server=${currentProxy.protocol}://${currentProxy.ip}:${currentProxy.port}`);
            }
            
            newBrowser = await puppeteer.launch({
                headless: 'new',
                userDataDir: userDataDir,
                args: args,
                ignoreHTTPSErrors: true,
                defaultViewport: null,
                timeout: 30000 // Aumentar timeout para 30 segundos
            });
            
            // Teste simplificado: abre uma página about:blank para verificar se o navegador está funcionando
            try {
                console.log(`[${new Date().toISOString()}] Testando navegador com about:blank...`);
                const testPage = await newBrowser.newPage();
                await testPage.goto('about:blank', { timeout: 10000 });
                await testPage.close();
                console.log(`[${new Date().toISOString()}] Teste básico do navegador bem-sucedido.`);
            } catch (testError) {
                console.error(`[${new Date().toISOString()}] Erro no teste do navegador:`, testError.message);
                // Se o teste falhar, fecha o navegador e tenta outro proxy
                if (newBrowser) {
                    await newBrowser.close().catch(() => {});
                }
                newBrowser = null;
                throw new Error(`Falha no teste do navegador: ${testError.message}`);
            }
            
            // Teste mais completo: tenta acessar uma página externa simples
            try {
                console.log(`[${new Date().toISOString()}] Testando navegador com site externo...`);
                const testPage = await newBrowser.newPage();
                // Usamos uma página simples e confiável para teste
                await testPage.goto('https://www.example.com', { 
                    timeout: 15000,
                    waitUntil: 'domcontentloaded' 
                });
                
                const pageContent = await testPage.content();
                const isWorking = pageContent.includes('Example Domain');
                
                await testPage.close();
                
                if (isWorking) {
                    console.log(`[${new Date().toISOString()}] Teste completo do navegador bem-sucedido.`);
                } else {
                    console.error(`[${new Date().toISOString()}] O navegador abriu, mas não conseguiu carregar o conteúdo da página de teste.`);
                    throw new Error('Falha ao carregar conteúdo no teste');
                }
            } catch (testError) {
                console.error(`[${new Date().toISOString()}] Erro no teste completo:`, testError.message);
                // Se o teste falhar, fecha o navegador e tenta outro proxy
                if (newBrowser) {
                    await newBrowser.close().catch(() => {});
                }
                newBrowser = null;
                throw new Error(`Falha no teste completo: ${testError.message}`);
            }
            
            console.log(`[${new Date().toISOString()}] Novo navegador inicializado com sucesso após rotação de proxy`);
            
        } catch (error) {
            console.error(`[${new Date().toISOString()}] Tentativa ${attemptCount} falhou:`, error.message);
            
            // Marca o proxy atual como não funcionando para não ser selecionado novamente
            if (currentProxy && currentProxy.protocol !== 'direct') {
                const proxyIndex = proxyPool.findIndex(p => 
                    p.ip === currentProxy.ip && p.port === currentProxy.port
                );
                
                if (proxyIndex !== -1) {
                    console.log(`[${new Date().toISOString()}] Marcando proxy ${currentProxy.ip}:${currentProxy.port} como não funcionando`);
                    proxyPool[proxyIndex].working = false;
                }
            }
            
            // Se for a última tentativa, vamos tentar conexão direta como último recurso
            if (attemptCount >= maxAttempts) {
                console.log(`[${new Date().toISOString()}] Todas as tentativas de proxy falharam. Tentando conexão direta como último recurso.`);
                
                try {
                    currentProxy = {
                        ip: null,
                        port: null,
                        protocol: 'direct',
                        lastUsed: new Date(),
                        working: true
                    };
                    
                    newBrowser = await puppeteer.launch({
                        headless: 'new',
                        userDataDir: userDataDir,
                        args: [
                            '--no-sandbox',
                            '--disable-setuid-sandbox',
                            '--disable-infobars',
                            '--disable-features=IsolateOrigins,site-per-process',
                            '--disable-blink-features=AutomationControlled',
                            `--user-agent=${getValidUserAgent()}`
                        ],
                        ignoreHTTPSErrors: true,
                        defaultViewport: null
                    });
                    
                    // Teste rápido
                    const testPage = await newBrowser.newPage();
                    await testPage.goto('about:blank');
                    await testPage.close();
                    
                    console.log(`[${new Date().toISOString()}] Conexão direta estabelecida com sucesso como fallback`);
                } catch (fallbackError) {
                    console.error(`[${new Date().toISOString()}] Erro fatal ao tentar conexão direta:`, fallbackError.message);
                    
                    // Última tentativa com configurações mínimas
                    console.log(`[${new Date().toISOString()}] Tentativa de último recurso com configurações mínimas`);
                    
                    try {
                        newBrowser = await puppeteer.launch({
                            headless: 'new',
                            args: ['--no-sandbox', '--disable-setuid-sandbox']
                        });
                        console.log(`[${new Date().toISOString()}] Navegador iniciado com configurações mínimas`);
                    } catch (finalError) {
                        console.error(`[${new Date().toISOString()}] Falha fatal na inicialização do browser:`, finalError.message);
                        throw new Error('Impossível inicializar o navegador após múltiplas tentativas');
                    }
                }
            }
        }
    }
    
    if (!newBrowser) {
        throw new Error('Falha ao inicializar navegador após múltiplas tentativas');
    }
    
    return newBrowser;
}

/**
 * Verifica se o browser está ativo e funcionando
 * @param {Browser} browser - Instância do navegador
 * @returns {Promise<boolean>} - true se o browser estiver funcionando
 */
async function isBrowserHealthy(browser) {
    if (!browser || browser._closed) {
        return false;
    }
    
    try {
        // Tenta abrir uma página básica para verificar se o browser responde
        const page = await browser.newPage();
        await page.goto('about:blank', { timeout: 5000 });
        await page.close();
        return true;
    } catch (error) {
        console.error(`[${new Date().toISOString()}] Browser health check failed: ${error.message}`);
        return false;
    }
}

/**
 * Realiza uma pesquisa no Google usando Puppeteer
 * @param {Browser} browser - Instância do navegador Puppeteer
 * @param {string} query - A consulta de pesquisa
 * @param {number} counter - Contador de CAPTCHAs
 * @returns {Promise<Object>} Resultado da pesquisa e contador atualizado
 */
async function searchGoogle(browser, query, counter) {
    let page;
    let browserWasRestarted = false;
    let maxRetries = 3; // Número máximo de tentativas
    
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
            // Verifica se o browser está saudável, se não, reinicializa
            if (!browser || browser._closed || !(await isBrowserHealthy(browser))) {
                console.log(`[${new Date().toISOString()}] Browser fechado, inválido ou não saudável. Inicializando novo browser (tentativa ${attempt}).`);
                
                // Se o browser existe mas não está saudável, tenta fechá-lo corretamente
                if (browser && !browser._closed) {
                    try {
                        await browser.close();
                        console.log(`[${new Date().toISOString()}] Browser existente fechado com sucesso.`);
                    } catch (closeError) {
                        console.error(`[${new Date().toISOString()}] Erro ao fechar browser existente: ${closeError.message}`);
                    }
                }
                
                // Pausa breve para garantir que quaisquer recursos sejam liberados
                await sleep(3000);
                
                // Reinicializa o browser completo com nova rotação de proxy
                browser = await rotateProxyAndRestartBrowser(null);
                browserWasRestarted = true;
                
                // Verificação extra para garantir que o novo browser está funcionando
                if (!(await isBrowserHealthy(browser))) {
                    throw new Error("Novo browser inicializado não está saudável");
                }
            }
            
            // Cria uma nova página
            console.log(`[${new Date().toISOString()}] Abrindo nova página para pesquisa (tentativa ${attempt}).`);
            page = await browser.newPage();
            await page.deleteCookie(...(await page.cookies()));
            await page.setBypassCSP(true); // Permite acesso a recursos restritos

            // Incrementa contador de requisições para o proxy atual
            proxyRequestCount++;

            // Verifica se precisamos rotacionar o proxy baseado no número de requisições
            if (proxyRequestCount >= MAX_REQUESTS_PER_PROXY) {
                console.log(`[${new Date().toISOString()}] Atingido limite de ${MAX_REQUESTS_PER_PROXY} requisições para o proxy atual. Rotacionando...`);
                await page.close();
                browser = await rotateProxyAndRestartBrowser(browser);
                page = await browser.newPage();
                await page.deleteCookie(...(await page.cookies()));
                await page.setBypassCSP(true);
            }

            // Substitua a limpeza do localStorage por:
            await page.evaluateOnNewDocument(() => {
                Object.defineProperty(window, 'localStorage', {
                    value: null,
                    writable: false
                });
                Object.defineProperty(window, 'sessionStorage', {
                    value: null,
                    writable: false
                });
            });
            
            await page.setUserAgent(getValidUserAgent());

            // Configura headers adicionais para parecer mais com um navegador real
            await page.setExtraHTTPHeaders({
                'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
                'DNT': '1',
                'Upgrade-Insecure-Requests': '1',
                'Cache-Control': 'max-age=0'
            });

            // Define um timeout mais longo para a navegação
            console.log(`[${new Date().toISOString()}] Navigating to Google with query: ${query}`);
            await page.goto(`https://www.google.com/search?q=${encodeURIComponent(query)}`, {
                waitUntil: 'networkidle2',
                timeout: 45000 // Aumentado para 45 segundos
            });

            // Adiciona um pequeno atraso para parecer mais humano
            await sleep(Math.floor(Math.random() * 5000) + 2000);

            // Verificação de CAPTCHA melhorada
            const pageUrl = page.url();
            const pageContent = await page.content();
            const isCaptchaPresent =
                pageUrl.includes('/sorry/') ||
                pageUrl.includes('captcha') ||
                pageContent.includes('unusual traffic') ||
                pageContent.includes('verify you') ||
                pageContent.includes('confirm you') ||
                pageContent.includes('not a robot') ||
                pageContent.includes('recaptcha') ||
                // Mensagens em português
                pageContent.includes('tráfego incomum') ||
                pageContent.includes('Nossos sistemas detectaram') ||
                pageContent.includes('Esta página verifica');

            if (isCaptchaPresent) {
                console.error(`[${new Date().toISOString()}] CAPTCHA DETECTED! Attempt #${counter + 1}`);
                counter++;

                // Fecha a página atual
                await page.close();

                // Rotaciona proxy após detecção de CAPTCHA
                console.log(`[${new Date().toISOString()}] Rotacionando proxy após detecção de CAPTCHA`);
                browser = await rotateProxyAndRestartBrowser(browser);

                if (counter >= 15) {
                    console.log(`[${new Date().toISOString()}] CAPTCHA detected 15 times in a row. Waiting 3 minutes...`);
                    // Espera 3 minutos antes de tentar novamente
                    const captchaWaitTime = 3 * 60 * 1000; // 3 minutos em milissegundos
                    await sleep(captchaWaitTime);
                    counter = 0; // Reset counter after waiting
                }

                // Reinicia o processo do começo
                console.log(`[${new Date().toISOString()}] Restarting process from beginning...`);

                return { captchaDetected: true, html: null, restartProcess: true, counter, browser };
            }

            // Obtém o HTML da página
            const html = await page.content();

            // Fecha a página (não o navegador)
            await page.close();

            return { captchaDetected: false, html, counter, browser };
            
        } catch (error) {
            console.error(`[${new Date().toISOString()}] Error during Puppeteer search (tentativa ${attempt}/${maxRetries}):`, error.message);

            // Garante que a página seja fechada mesmo em caso de erro
            if (page && !page.isClosed()) {
                try {
                    await page.close();
                } catch (closeError) {
                    console.error(`[${new Date().toISOString()}] Error closing page:`, closeError.message);
                }
            }

            // Verifica se o erro é relacionado à conexão fechada ou problemas de protocolo
            const criticalErrors = [
                'Protocol error',
                'Connection closed',
                'Target closed',
                'WebSocket',
                'not opened',
                'crashed',
                'terminated',
                'disconnected'
            ];
            
            const isCriticalError = criticalErrors.some(errorText => 
                error.message.includes(errorText)
            );

            // Para erros críticos, sempre reinicializa o browser
            if (isCriticalError) {
                console.log(`[${new Date().toISOString()}] Erro crítico detectado. Forçando reinicialização do browser...`);
                
                // Tenta fechar o browser atual se existir
                if (browser && !browser._closed) {
                    try {
                        await browser.close();
                    } catch (closeError) {
                        console.error(`[${new Date().toISOString()}] Erro ao fechar browser após erro crítico:`, closeError.message);
                    }
                }
                
                // Pausa longa para garantir que o sistema se recupere
                await sleep(5000);
                
                // Reinicializa o browser com novo proxy
                browser = await rotateProxyAndRestartBrowser(null);
                
                // Se não é a última tentativa, continua para a próxima
                if (attempt < maxRetries) {
                    console.log(`[${new Date().toISOString()}] Tentando novamente após reinicialização (tentativa ${attempt + 1}/${maxRetries})...`);
                    continue;
                }
            }
            
            // Verifica se o erro pode estar relacionado ao proxy
            const proxyRelatedErrors = [
                'net::',
                'ERR_PROXY_CONNECTION_FAILED',
                'ERR_TUNNEL_CONNECTION_FAILED',
                'ERR_CONNECTION_RESET',
                'ERR_CONNECTION_CLOSED',
                'ERR_CONNECTION_TIMED_OUT',
                'ERR_CONNECTION_REFUSED',
                'ERR_NETWORK_CHANGED',
                'timeout'
            ];

            const needsProxyRotation = proxyRelatedErrors.some(errorText => 
                error.message.includes(errorText)
            );

            if (needsProxyRotation) {
                console.log(`[${new Date().toISOString()}] Possível problema com proxy. Rotacionando...`);
                browser = await rotateProxyAndRestartBrowser(browser);
                
                // Se não é a última tentativa, continua para a próxima
                if (attempt < maxRetries) {
                    console.log(`[${new Date().toISOString()}] Tentando novamente após rotação de proxy (tentativa ${attempt + 1}/${maxRetries})...`);
                    continue;
                }
            }
            
            // Se chegou aqui e é a última tentativa, lança o erro para fora
            if (attempt >= maxRetries) {
                throw { error: `Falha após ${maxRetries} tentativas: ${error.message}`, browser };
            }
        }
    }
    
    // Este código só deve ser alcançado se todas as tentativas falharem
    throw { error: `Falha após ${maxRetries} tentativas sem erro específico`, browser };
}

/**
 * Visita o site da empresa usando Puppeteer
 * @param {Browser} browser - Instância do navegador Puppeteer
 * @param {string} site - URL do site da empresa
 * @returns {Promise<Object>} HTML do site e referência atualizada do browser
 */
async function visitCompanySite(browser, site) {
    let page;
    let maxRetries = 3;
    
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
            // Verifica se o browser está saudável
            if (!browser || browser._closed || !(await isBrowserHealthy(browser))) {
                console.log(`[${new Date().toISOString()}] Browser fechado, inválido ou não saudável. Inicializando novo browser (tentativa ${attempt}).`);
                
                // Se existe um browser, tenta fechá-lo corretamente
                if (browser && !browser._closed) {
                    try {
                        await browser.close();
                    } catch (closeError) {
                        console.error(`[${new Date().toISOString()}] Erro ao fechar browser existente:`, closeError.message);
                    }
                }
                
                await sleep(3000);
                browser = await rotateProxyAndRestartBrowser(null);
                
                // Verificação extra para garantir que o novo browser está funcionando
                if (!(await isBrowserHealthy(browser))) {
                    throw new Error("Novo browser inicializado não está saudável");
                }
            }
            
            page = await browser.newPage();
            await page.deleteCookie(...(await page.cookies()));
            await page.setBypassCSP(true);

            // Incrementa contador de requisições para o proxy atual
            proxyRequestCount++;

            // Verifica se precisamos rotacionar o proxy
            if (proxyRequestCount >= MAX_REQUESTS_PER_PROXY) {
                console.log(`[${new Date().toISOString()}] Atingido limite de ${MAX_REQUESTS_PER_PROXY} requisições para o proxy. Rotacionando...`);
                await page.close();
                browser = await rotateProxyAndRestartBrowser(browser);
                page = await browser.newPage();
                await page.deleteCookie(...(await page.cookies()));
                await page.setBypassCSP(true);
            }

            await page.setUserAgent(getValidUserAgent());

            // Adiciona headers para parecer mais com um navegador real
            await page.setExtraHTTPHeaders({
                'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
                'DNT': '1',
                'Upgrade-Insecure-Requests': '1',
                'Cache-Control': 'max-age=0'
            });

            // Corrige a URL se necessário
            let url = site;
            if (!url.startsWith('http')) {
                url = 'https://' + url;
            }

            console.log(`[${new Date().toISOString()}] Visiting company site: ${url} (tentativa ${attempt}/${maxRetries})`);

            // Aumenta timeout para navegação
            await page.goto(url, {
                waitUntil: 'networkidle2',
                timeout: 30000
            });

            // Adiciona um pequeno atraso para carregar conteúdo dinâmico
            await sleep(Math.floor(Math.random() * 2000) + 1000);

            // Obtém o HTML da página
            const html = await page.content();

            // Fecha a página (não o navegador)
            await page.close();

            return { html, browser };
            
        } catch (error) {
            console.error(`[${new Date().toISOString()}] Error visiting company site (tentativa ${attempt}/${maxRetries}):`, error.message);

            // Garante que a página seja fechada mesmo em caso de erro
            if (page && !page.isClosed()) {
                try {
                    await page.close();
                } catch (closeError) {
                    console.error(`[${new Date().toISOString()}] Error closing page:`, closeError.message);
                }
            }

            // Verifica se o erro é relacionado à conexão fechada ou problemas de protocolo
            const criticalErrors = [
                'Protocol error',
                'Connection closed',
                'Target closed',
                'WebSocket',
                'not opened',
                'crashed',
                'terminated',
                'disconnected'
            ];
            
            const isCriticalError = criticalErrors.some(errorText => 
                error.message.includes(errorText)
            );

            // Para erros críticos, sempre reinicializa o browser
            if (isCriticalError) {
                console.log(`[${new Date().toISOString()}] Erro crítico detectado. Forçando reinicialização do browser...`);
                
                // Tenta fechar o browser atual se existir
                if (browser && !browser._closed) {
                    try {
                        await browser.close();
                    } catch (closeError) {
                        console.error(`[${new Date().toISOString()}] Erro ao fechar browser após erro crítico:`, closeError.message);
                    }
                }
                
                await sleep(5000);
                browser = await rotateProxyAndRestartBrowser(null);
                
                // Se não é a última tentativa, continua para a próxima
                if (attempt < maxRetries) {
                    console.log(`[${new Date().toISOString()}] Tentando novamente após reinicialização (tentativa ${attempt + 1}/${maxRetries})...`);
                    continue;
                }
            }
            
            // Verifica se o erro pode estar relacionado ao proxy
            const proxyRelatedErrors = [
                'net::',
                'ERR_PROXY_CONNECTION_FAILED',
                'ERR_TUNNEL_CONNECTION_FAILED',
                'ERR_CONNECTION_RESET',
                'ERR_CONNECTION_CLOSED',
                'ERR_CONNECTION_TIMED_OUT',
                'ERR_CONNECTION_REFUSED',
                'ERR_NETWORK_CHANGED',
                'timeout'
            ];

            const needsProxyRotation = proxyRelatedErrors.some(errorText => 
                error.message.includes(errorText)
            );

            if (needsProxyRotation) {
                console.log(`[${new Date().toISOString()}] Possível problema com proxy. Rotacionando...`);
                browser = await rotateProxyAndRestartBrowser(browser);
                
                if (attempt < maxRetries) {
                    console.log(`[${new Date().toISOString()}] Tentando novamente após rotação de proxy (tentativa ${attempt + 1}/${maxRetries})...`);
                    continue;
                }
            }
            
            // Se é a última tentativa, lança o erro
            if (attempt >= maxRetries) {
                throw { error: `Falha após ${maxRetries} tentativas: ${error.message}`, browser };
            }
        }
    }
    
    throw { error: `Falha após ${maxRetries} tentativas sem erro específico`, browser };
}

// Na função principal (IIFE), modifique:
(async () => {
    let captchaCounter = 0;
    let browser = null;

    // Inicializa o pool de proxies primeiro
    await initializeProxyPool();
    
    // Depois inicializa o navegador
    browser = await initBrowser();

    async function startScraping() {
        try {
            // Inicializa o navegador uma única vez
            console.log(`[${new Date().toISOString()}] Initializing browser...`);
            console.log(`[${new Date().toISOString()}] Browser initialized successfully!`);

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
                            // Visita o site da empresa com Puppeteer usando o navegador já aberto
                            const siteResult = await visitCompanySite(browser, bd1.site);
                            const siteHtml = siteResult.html;
                            // Atualiza a referência do browser se foi retornada
                            if (siteResult.browser) {
                                browser = siteResult.browser;
                            }
                            
                            const $site = cheerio.load(siteHtml);

                            // Extrai todo o texto do site
                            const siteText = $site('body').text();

                            // Verificações e extração de dados seguem como antes
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

                                // Busca email se ainda não temos
                                if (!contato.email) {
                                    // Tenta extrair email do texto principal
                                    const emailEncontrado = extrairEmail(siteText);
                                    if (emailEncontrado) {
                                        contato.email = emailEncontrado;
                                        console.log(`[${new Date().toISOString()}] Email encontrado no texto do site: ${contato.email}`);
                                    } else {
                                        // Buscas em elementos específicos do site continuam como antes
                                        // Seletores comuns para áreas de contato
                                        const contatoSeletores = [
                                            'a[href^="mailto:"]',                 // Links de email
                                            '.contact', '.contato',               // Classes comuns para áreas de contato
                                            '#contact', '#contato',               // IDs comuns para áreas de contato
                                            'footer a', '.footer a',              // Links em rodapés
                                            '.email', '#email',                   // Classes/IDs específicos para email
                                            '[itemprop="email"]',                 // Marcação de microdata
                                            '.contact-info', '.info-contato'      // Outras classes comuns
                                        ];

                                        // Procura por elementos específicos que geralmente contêm emails
                                        for (const seletor of contatoSeletores) {
                                            const elementos = $site(seletor);
                                            if (elementos.length > 0) {
                                                console.log(`[${new Date().toISOString()}] Encontrado ${elementos.length} elementos com seletor "${seletor}"`);

                                                for (let i = 0; i < elementos.length; i++) {
                                                    const elemento = elementos[i];

                                                    // Se for um link mailto, extrai diretamente
                                                    const href = $site(elemento).attr('href');
                                                    if (href && href.toLowerCase().startsWith('mailto:')) {
                                                        const emailFromMailto = href.substring(7).split('?')[0].trim();
                                                        if (emailFromMailto && emailFromMailto.includes('@')) {
                                                            contato.email = emailFromMailto;
                                                            console.log(`[${new Date().toISOString()}] Email encontrado em link mailto: ${contato.email}`);
                                                            break;
                                                        }
                                                    }

                                                    // Verifica o texto do elemento
                                                    const textoElemento = $site(elemento).text();
                                                    const emailNoElemento = extrairEmail(textoElemento);
                                                    if (emailNoElemento) {
                                                        contato.email = emailNoElemento;
                                                        console.log(`[${new Date().toISOString()}] Email encontrado em elemento "${seletor}": ${contato.email}`);
                                                        break;
                                                    }
                                                }

                                                if (contato.email) break; // Se já encontrou email, para a busca
                                            }
                                        }

                                        // Se ainda não encontrou, procura por links para páginas de contato
                                        if (!contato.email) {
                                            console.log(`[${new Date().toISOString()}] Buscando links para páginas de contato...`);
                                            const contatoLinks = $site('a').filter(function () {
                                                const texto = $site(this).text().toLowerCase();
                                                return texto.includes('contato') ||
                                                    texto.includes('contact') ||
                                                    texto.includes('fale conosco') ||
                                                    texto.includes('atendimento');
                                            });

                                            if (contatoLinks.length > 0) {
                                                console.log(`[${new Date().toISOString()}] Encontrado ${contatoLinks.length} links potenciais para páginas de contato`);
                                                // Aqui poderíamos implementar uma visita à página de contato, mas isso aumentaria o número de requisições
                                                // e poderia tornar o scraping mais lento e mais facilmente detectável
                                            }
                                        }
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
                        }
                    }
                }

                // Se ainda precisamos buscar no Google (site não existente, erro ao acessar, ou faltam dados)
                if (needGoogleSearch) {
                    try {
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
                            hasProcessingError = true;
                            continue;
                        }

                        const bd2 = result2.rows[0];

                        // Monta a consulta para o Google com base nas informações da empresa
                        const empresaQuery = `${bd2.trade_name || bd2.name} ${bd2.address_city_name || ''} ${bd2.address_fu || ''}`;
                        const query = `${empresaQuery} ${bd1.cnpj} contato telefone email site`;

                        console.log(`[${new Date().toISOString()}] Searching Google for: ${query}`);

                        try {
                            // Utiliza o Puppeteer para buscar no Google, passando o navegador já aberto e o contador
                            const searchResult = await searchGoogle(browser, query, captchaCounter);

                            // Atualiza o contador com o valor retornado
                            captchaCounter = searchResult.counter;
                            
                            // Atualiza a referência do browser se foi retornada
                            if (searchResult.browser) {
                                browser = searchResult.browser;
                            }

                            // Verifica se foi detectado CAPTCHA
                            if (searchResult.captchaDetected) {
                                console.error(`[${new Date().toISOString()}] CAPTCHA DETECTED!`);

                                // Verifica se precisamos reiniciar todo o processo
                                if (searchResult.restartProcess) {
                                    console.log(`[${new Date().toISOString()}] Restarting entire scraping process...`);
                                    return await startScraping(); // Reinicia todo o processo
                                }

                                // Se não recebeu flag de reinício, apenas pula para o próximo registro
                                console.log(`[${new Date().toISOString()}] Skipping this record... (CAPTCHA count: ${captchaCounter})`);
                                continue; // Pula para a próxima iteração do loop principal
                            }

                            const html = searchResult.html;

                            // Verifica se o HTML é válido antes de prosseguir
                            if (!html) {
                                console.error(`[${new Date().toISOString()}] Invalid or empty HTML returned from search`);
                                hasProcessingError = true;
                                continue; // Pula para a próxima iteração do loop principal
                            }

                            // Carrega o HTML com o Cheerio para análise
                            console.log(`[${new Date().toISOString()}] Loading search results HTML with Cheerio`);
                            const $ = cheerio.load(html);

                            // O restante do processamento continua igual, usando cheerio para analisar o HTML

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
                                                    if (companyWords[i].length > 2 && companyWords[i + 1].length > 2) {
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
                                        if (emailEncontrado) {
                                            contato.email = emailEncontrado;
                                            console.log(`[${new Date().toISOString()}] Email complementado da página do Google: ${contato.email}`);
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
                                if (emailEncontrado) {
                                    contato.email = emailEncontrado;
                                    console.log(`[${new Date().toISOString()}] Email encontrado nos resultados do Google: ${contato.email}`);
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
                                        if (blockEmail) {
                                            contato.email = blockEmail;
                                            console.log(`[${new Date().toISOString()}] Email encontrado em bloco de informação do Google: ${contato.email}`);
                                        }
                                    }
                                });
                            }
                        } catch (error) {
                            console.error(`[${new Date().toISOString()}] Error fetching search results:`, error.message);

                            // Remover tratamento específico para erro 429, apenas tratar como erro genérico
                            console.log(`[${new Date().toISOString()}] Connection issue or access denied. Skipping to next record.`);
                            await sleep(5000); // Pequena pausa antes de continuar
                            hasProcessingError = true; // Marca como erro
                            continue; // Pula para a próxima iteração do loop principal
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

                                if (contato.email && (!bd1.email || bd1.email === '')) {
                                    fieldsToUpdate.push(`email = $${paramIndex}`);
                                    valuesToUpdate.push(contato.email);
                                    paramIndex++;
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
            // Fecha o navegador no final de tudo
            if (browser) {
                try {
                    await browser.close();
                    console.log(`[${new Date().toISOString()}] Browser closed successfully.`);
                } catch (browserError) {
                    console.error(`[${new Date().toISOString()}] Error closing browser:`, browserError.message);
                }
            }

            // Fecha conexões de banco de dados
            try {
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