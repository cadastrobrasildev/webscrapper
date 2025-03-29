const axios = require('axios');
require('dotenv').config();

const CAPSOLVER_API_URL = 'https://api.capsolver.com';
const DEFAULT_TIMEOUT = 180000; // 3 minutes timeout

/**
 * Creates a task on CapSolver API to solve Google reCAPTCHA
 * @param {string} apiKey - Your CapSolver API key
 * @param {string} websiteURL - URL of the website with CAPTCHA
 * @param {string} websiteKey - Google reCAPTCHA site key
 * @param {string} [type='reCaptchaV2Task'] - Type of CAPTCHA task
 * @returns {Promise<string>} Task ID from CapSolver
 */
async function createCaptchaTask(apiKey, websiteURL, websiteKey, type = 'ReCaptchaV2Task') {
    try {
        console.log(`[${new Date().toISOString()}] Creating ${type} task for ${websiteURL}`);
        
        const response = await axios.post(`${CAPSOLVER_API_URL}/createTask`, {
            clientKey: apiKey,
            task: {
                type: type,
                websiteURL: websiteURL,
                websiteKey: websiteKey,
                // Optional parameters for specific cases
                isInvisible: false,
                userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
        });

        if (response.data.errorId > 0) {
            throw new Error(`CapSolver API error: ${response.data.errorDescription}`);
        }

        return response.data.taskId;
    } catch (error) {
        console.error(`[${new Date().toISOString()}] Error creating CAPTCHA task:`, error.message);
        throw error;
    }
}

/**
 * Gets the result of a CAPTCHA solving task
 * @param {string} apiKey - Your CapSolver API key
 * @param {string} taskId - The task ID to check
 * @returns {Promise<Object>} Task result data
 */
async function getTaskResult(apiKey, taskId) {
    try {
        console.log(`[${new Date().toISOString()}] Checking task result for task ${taskId}`);
        
        const response = await axios.post(`${CAPSOLVER_API_URL}/getTaskResult`, {
            clientKey: apiKey,
            taskId: taskId
        });

        if (response.data.errorId > 0) {
            throw new Error(`CapSolver API error: ${response.data.errorDescription}`);
        }

        return response.data;
    } catch (error) {
        console.error(`[${new Date().toISOString()}] Error getting task result:`, error.message);
        throw error;
    }
}

/**
 * Gets the balance of your CapSolver account
 * @param {string} apiKey - Your CapSolver API key
 * @returns {Promise<number>} Account balance
 */
async function getBalance(apiKey) {
    try {
        const response = await axios.post(`${CAPSOLVER_API_URL}/getBalance`, {
            clientKey: apiKey
        });

        if (response.data.errorId > 0) {
            throw new Error(`CapSolver API error: ${response.data.errorDescription}`);
        }

        return response.data.balance;
    } catch (error) {
        console.error(`[${new Date().toISOString()}] Error checking balance:`, error.message);
        throw error;
    }
}

/**
 * Solves Google reCAPTCHA using CapSolver API
 * @param {Object} options - Configuration options
 * @param {string} options.apiKey - Your CapSolver API key
 * @param {string} options.websiteURL - URL of the website with CAPTCHA
 * @param {string} options.websiteKey - Google reCAPTCHA site key
 * @param {string} [options.type='ReCaptchaV2Task'] - Type of CAPTCHA task
 * @param {number} [options.timeout=180000] - Timeout in milliseconds
 * @param {number} [options.pollingInterval=3000] - Time between result checks in milliseconds
 * @returns {Promise<string>} CAPTCHA solution token
 */
async function solveCaptcha(options) {
    const {
        apiKey = process.env.CAPSOLVER_API_KEY,
        websiteURL,
        websiteKey,
        type = 'ReCaptchaV2Task',
        timeout = DEFAULT_TIMEOUT,
        pollingInterval = 3000
    } = options;

    if (!apiKey) {
        throw new Error('CapSolver API key is required. Provide it in options or set CAPSOLVER_API_KEY in .env file.');
    }

    if (!websiteURL || !websiteKey) {
        throw new Error('Website URL and reCAPTCHA website key are required.');
    }

    try {
        // Check balance first
        const balance = await getBalance(apiKey);
        console.log(`[${new Date().toISOString()}] Current CapSolver balance: $${balance}`);
        
        if (balance <= 0) {
            throw new Error('Insufficient balance in your CapSolver account.');
        }

        // Create a task to solve the CAPTCHA
        const taskId = await createCaptchaTask(apiKey, websiteURL, websiteKey, type);
        console.log(`[${new Date().toISOString()}] CAPTCHA task created with ID: ${taskId}`);

        // Set timeout for the whole operation
        const startTime = Date.now();
        
        // Poll for the result
        while (Date.now() - startTime < timeout) {
            const resultData = await getTaskResult(apiKey, taskId);
            
            // Check if the task is ready
            if (resultData.status === 'ready') {
                console.log(`[${new Date().toISOString()}] CAPTCHA solved successfully!`);
                return resultData.solution.gRecaptchaResponse;
            }
            
            if (resultData.status === 'failed') {
                throw new Error(`CAPTCHA solving failed: ${resultData.errorDescription || 'Unknown error'}`);
            }
            
            // If not ready, wait before checking again
            console.log(`[${new Date().toISOString()}] Task status: ${resultData.status}, waiting ${pollingInterval}ms...`);
            await new Promise(resolve => setTimeout(resolve, pollingInterval));
        }
        
        throw new Error(`Timeout reached (${timeout}ms) while waiting for CAPTCHA solution`);
    } catch (error) {
        console.error(`[${new Date().toISOString()}] CAPTCHA solving error:`, error.message);
        throw error;
    }
}

/**
 * Detects and extracts Google reCAPTCHA sitekey from a webpage
 * @param {Object} page - Puppeteer page object
 * @returns {Promise<string|null>} The detected sitekey or null if not found
 */
async function detectRecaptchaSitekey(page) {
    try {
        // Look for reCAPTCHA sitekey in the page
        const sitekey = await page.evaluate(() => {
            // Method 1: Check for g-recaptcha div with data-sitekey attribute
            const recaptchaDiv = document.querySelector('.g-recaptcha');
            if (recaptchaDiv && recaptchaDiv.dataset.sitekey) {
                return recaptchaDiv.dataset.sitekey;
            }
            
            // Method 2: Check for recaptcha/api.js script with sitekey parameter
            const scripts = Array.from(document.getElementsByTagName('script'));
            for (const script of scripts) {
                if (script.src.includes('recaptcha/api.js')) {
                    const match = script.src.match(/[?&]render=([^&]+)/);
                    if (match && match[1] && match[1] !== 'explicit') {
                        return match[1];
                    }
                }
            }
            
            // Method 3: Look in grecaptcha object if it's already loaded
            if (window.grecaptcha && window.grecaptcha.enterprise) {
                const keys = Object.keys(window.grecaptcha.enterprise);
                for (const key of keys) {
                    if (key.length > 30) return key;
                }
            }
            
            return null;
        });
        
        if (sitekey) {
            console.log(`[${new Date().toISOString()}] Detected reCAPTCHA sitekey: ${sitekey}`);
            return sitekey;
        }
        
        console.log(`[${new Date().toISOString()}] No reCAPTCHA sitekey detected on the page`);
        return null;
    } catch (error) {
        console.error(`[${new Date().toISOString()}] Error detecting reCAPTCHA sitekey:`, error.message);
        return null;
    }
}

/**
 * Automatically handles CAPTCHA detection and solving
 * @param {Object} page - Puppeteer page object
 * @param {string} apiKey - CapSolver API key
 * @returns {Promise<boolean>} True if CAPTCHA was solved, false otherwise
 */
async function handleCaptchaIfPresent(page, apiKey = process.env.CAPSOLVER_API_KEY) {
    try {
        const url = page.url();
        
        // Check if the page contains CAPTCHA indicators
        const isCaptchaPresent = await page.evaluate(() => {
            return document.body.textContent.includes('unusual traffic') ||
                   document.body.textContent.includes('verify you') ||
                   document.body.textContent.includes('not a robot') ||
                   document.body.textContent.includes('recaptcha') ||
                   document.body.textContent.includes('tráfego incomum') ||
                   document.body.textContent.includes('Nossos sistemas detectaram') ||
                   document.querySelector('.g-recaptcha') !== null ||
                   document.querySelector('iframe[src*="recaptcha"]') !== null;
        });
        
        if (!isCaptchaPresent) {
            return false;
        }
        
        console.log(`[${new Date().toISOString()}] CAPTCHA detected on page: ${url}`);
        
        // Detect the reCAPTCHA sitekey
        const sitekey = await detectRecaptchaSitekey(page);
        
        if (!sitekey) {
            console.error(`[${new Date().toISOString()}] Could not detect reCAPTCHA sitekey`);
            return false;
        }
        
        // Solve the CAPTCHA
        const captchaToken = await solveCaptcha({
            apiKey,
            websiteURL: url,
            websiteKey: sitekey
        });
        
        // Apply the solution to the page
        const success = await page.evaluate((token) => {
            try {
                // Method 1: Using grecaptcha.callback
                if (window.___grecaptcha_cfg && window.___grecaptcha_cfg.clients) {
                    const clientIds = Object.keys(window.___grecaptcha_cfg.clients);
                    for (const id of clientIds) {
                        const client = window.___grecaptcha_cfg.clients[id];
                        const elements = Object.keys(client).filter(key => key.includes('callback'));
                        for (const key of elements) {
                            if (typeof client[key] === 'function') {
                                client[key](token);
                                return true;
                            }
                        }
                    }
                }
                
                // Method 2: Find and submit the form
                if (document.querySelector('.g-recaptcha')) {
                    const form = document.querySelector('.g-recaptcha').closest('form');
                    if (form) {
                        // Create hidden input for g-recaptcha-response
                        const input = document.createElement('textarea');
                        input.setAttribute('name', 'g-recaptcha-response');
                        input.value = token;
                        input.style.display = 'none';
                        form.appendChild(input);
                        
                        // Submit the form
                        form.submit();
                        return true;
                    }
                }
                
                // Method 3: Set the response in text area (for invisible reCAPTCHA)
                const textarea = document.querySelector('textarea#g-recaptcha-response');
                if (textarea) {
                    textarea.value = token;
                    return true;
                }
                
                return false;
            } catch (e) {
                console.error('Error applying CAPTCHA solution:', e);
                return false;
            }
        }, captchaToken);
        
        if (success) {
            console.log(`[${new Date().toISOString()}] CAPTCHA solution applied successfully`);
            
            // Wait for navigation after submitting the form
            try {
                await page.waitForNavigation({ timeout: 10000 });
            } catch (e) {
                console.log(`[${new Date().toISOString()}] No navigation occurred after applying CAPTCHA solution`);
            }
            
            return true;
        } else {
            console.error(`[${new Date().toISOString()}] Failed to apply CAPTCHA solution to the page`);
            return false;
        }
    } catch (error) {
        console.error(`[${new Date().toISOString()}] Error handling CAPTCHA:`, error.message);
        return false;
    }
}

module.exports = {
    solveCaptcha,
    getBalance,
    detectRecaptchaSitekey,
    handleCaptchaIfPresent
};
