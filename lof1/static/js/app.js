// LOF基金套利工具 - 前端JavaScript

let autoRefreshInterval = null;
let updateInterval = 60; // 默认60秒
let favoriteFunds = new Set(); // 自选基金集合
let showFavoritesOnly = false; // 是否只显示自选基金
let hideDelistedFunds = true; // 是否隐藏场内退市基金
let currentUser = null; // 当前登录用户
let notificationCheckInterval = null; // 通知检查定时器

// 初始化
document.addEventListener('DOMContentLoaded', async function() {
    
    try {
        initEventListeners();
        
        loadConfig();
        
        await checkAuthStatus(); // 检查登录状态（先检查登录状态）
        
        await loadFavoriteFunds(); // 加载自选基金列表（登录状态检查后再加载）
        await loadUnreadNotificationCount(); // 加载未读通知数量
        startNotificationCheck(); // 开始定期检查通知
        
        loadFunds();
        
    } catch (error) {
        console.error('页面初始化失败:', error);
        
        // 如果是函数未定义错误，尝试延迟初始化
        if (error.message && error.message.includes('is not defined')) {
            console.warn('检测到函数未定义错误，尝试延迟初始化...');
            setTimeout(async () => {
                try {
                    // 重新尝试初始化事件监听器
                    if (typeof initEventListeners === 'function') {
                        initEventListeners();
                    }
                    // 尝试加载基金列表
                    if (typeof loadFunds === 'function') {
                        await loadFunds();
                    }
                } catch (retryError) {
                    console.error('延迟初始化也失败:', retryError);
                    alert('页面初始化失败: ' + retryError.message);
                }
            }, 500);
        } else {
            alert('页面初始化失败: ' + error.message);
        }
    }
});

// 初始化事件监听
function initEventListeners() {
    
    // 安全地绑定事件监听器（检查元素是否存在）
    const safeAddEventListener = (id, event, handler) => {
        const element = document.getElementById(id);
        if (element) {
            element.addEventListener(event, handler);
        } else {
            console.warn(`元素 ${id} 不存在，跳过事件绑定`);
        }
    };
    
    safeAddEventListener('refreshBtn', 'click', loadFunds);
    safeAddEventListener('autoRefreshBtn', 'click', toggleAutoRefresh);
    safeAddEventListener('discoverBtn', 'click', discoverFunds);
    // 检查函数是否存在再绑定
    if (typeof openArbitrageRecords === 'function') {
        safeAddEventListener('arbitrageRecordsBtn', 'click', openArbitrageRecords);
    } else {
        console.warn('openArbitrageRecords函数未定义，延迟绑定');
        // 延迟绑定：等待脚本加载完成
        setTimeout(() => {
            if (typeof openArbitrageRecords === 'function') {
                safeAddEventListener('arbitrageRecordsBtn', 'click', openArbitrageRecords);
            } else {
                console.error('openArbitrageRecords函数仍未定义');
            }
        }, 100);
    }
    safeAddEventListener('settingsBtn', 'click', () => window.openSettings());
    safeAddEventListener('donateBtn', 'click', openDonate);
    safeAddEventListener('dataSourceConfigBtn', 'click', openDataSourceConfig);
    // 注册表单实时验证
    const registerUsernameInput = document.getElementById('registerUsername');
    const registerEmailInput = document.getElementById('registerEmail');
    if (registerUsernameInput) {
        registerUsernameInput.addEventListener('input', function() {
            validateUsername(this.value);
        });
        registerUsernameInput.addEventListener('blur', function() {
            validateUsername(this.value);
        });
    }
    if (registerEmailInput) {
        registerEmailInput.addEventListener('input', function() {
            validateEmail(this.value);
        });
        registerEmailInput.addEventListener('blur', function() {
            validateEmail(this.value);
        });
    }
    safeAddEventListener('adminArbitrageRecordsBtn', 'click', function(e) {
        e.preventDefault();
        openAdminArbitrageRecords();
    });
    safeAddEventListener('closeAdminArbitrageRecordsBtn', 'click', closeAdminArbitrageRecords);
    safeAddEventListener('closeAdminArbitrageRecordsFooterBtn', 'click', closeAdminArbitrageRecords);
    safeAddEventListener('refreshAdminArbitrageRecordsBtn', 'click', loadAdminArbitrageRecords);
    safeAddEventListener('applyAdminArbitrageFilterBtn', 'click', applyAdminArbitrageFilter);
    safeAddEventListener('notificationBtn', 'click', openNotifications);
    safeAddEventListener('closeNotificationBtn', 'click', closeNotifications);
    safeAddEventListener('closeNotificationFooterBtn', 'click', closeNotifications);
    safeAddEventListener('markAllReadBtn', 'click', markAllNotificationsRead);
    safeAddEventListener('deleteReadBtn', 'click', deleteAllReadNotifications);
    safeAddEventListener('closeSettingsBtn', 'click', closeSettings);
    safeAddEventListener('closeDonateBtn', 'click', closeDonate);
    safeAddEventListener('cancelDonateBtn', 'click', closeDonate);
    safeAddEventListener('closeDataSourceConfigBtn', 'click', closeDataSourceConfig);
    safeAddEventListener('cancelDataSourceConfigBtn', 'click', closeDataSourceConfig);
    safeAddEventListener('saveDataSourceConfigBtn', 'click', saveDataSourceConfig);
    safeAddEventListener('cancelSettingsBtn', 'click', closeSettings);
    safeAddEventListener('saveSettingsBtn', 'click', () => window.saveSettings());
    
    // 手机端菜单控制
    safeAddEventListener('mobileMenuBtn', 'click', openMobileMenu);
    safeAddEventListener('closeMobileMenuBtn', 'click', closeMobileMenu);
    
    // 点击菜单外部关闭菜单
    const mobileMenu = document.getElementById('mobileMenu');
    if (mobileMenu) {
        mobileMenu.addEventListener('click', function(e) {
            if (e.target === mobileMenu) {
                closeMobileMenu();
            }
        });
    }
    
    // 根据屏幕尺寸显示/隐藏菜单按钮
    function checkMobileMenu() {
        const mobileMenuBtn = document.getElementById('mobileMenuBtn');
        if (mobileMenuBtn && window.innerWidth <= 768) {
            mobileMenuBtn.style.display = 'inline-block';
            // 隐藏次要按钮
            const buttonsToHide = ['autoRefreshBtn', 'discoverBtn', 'arbitrageRecordsBtn', 'settingsBtn', 'donateBtn'];
            buttonsToHide.forEach(id => {
                const btn = document.getElementById(id);
                if (btn) btn.style.display = 'none';
            });
        } else if (mobileMenuBtn) {
            mobileMenuBtn.style.display = 'none';
            // 显示所有按钮
            const buttonsToShow = ['autoRefreshBtn', 'discoverBtn', 'arbitrageRecordsBtn', 'settingsBtn', 'donateBtn'];
            buttonsToShow.forEach(id => {
                const btn = document.getElementById(id);
                if (btn) btn.style.display = 'inline-flex';
            });
        }
    }
    
    window.addEventListener('resize', checkMobileMenu);
    checkMobileMenu();
    
    // 滚动时隐藏搜索栏
    let lastScrollTop = 0;
    const statsBar = document.querySelector('.stats-bar');
    window.addEventListener('scroll', function() {
        const scrollTop = window.pageYOffset || document.documentElement.scrollTop;
        if (statsBar) {
            if (scrollTop > lastScrollTop && scrollTop > 100) {
                // 向下滚动，隐藏搜索栏
                statsBar.classList.add('hidden');
            } else {
                // 向上滚动，显示搜索栏
                statsBar.classList.remove('hidden');
            }
        }
        lastScrollTop = scrollTop <= 0 ? 0 : scrollTop;
    }, false);
    safeAddEventListener('clearLogBtn', 'click', clearLog);
    safeAddEventListener('filterFavoritesBtn', 'click', toggleFavoritesFilter);
    safeAddEventListener('hideDelistedBtn', 'click', toggleDelistedFilter);

    // 基金搜索功能
    const fundSearchInput = document.getElementById('fundSearchInput');
    const clearSearchBtn = document.getElementById('clearSearchBtn');
    if (fundSearchInput) {
        fundSearchInput.addEventListener('input', function() {
            searchKeyword = this.value;
            // 显示/隐藏清除按钮
            if (clearSearchBtn) {
                clearSearchBtn.style.display = searchKeyword.trim() ? 'inline-block' : 'none';
            }
            // 重新显示基金列表（会自动应用搜索过滤）
            if (allFundsData.length > 0) {
                displayFunds(allFundsData);
            }
        });
        
        // 支持回车键搜索
        fundSearchInput.addEventListener('keypress', function(e) {
            if (e.key === 'Enter') {
                e.preventDefault();
                this.blur(); // 失去焦点
            }
        });
    }
    
    if (clearSearchBtn) {
        clearSearchBtn.addEventListener('click', function() {
            if (fundSearchInput) {
                fundSearchInput.value = '';
                searchKeyword = '';
                this.style.display = 'none';
                // 重新显示基金列表
                if (allFundsData.length > 0) {
                    displayFunds(allFundsData);
                }
            }
        });
    }
    
    // 用户认证相关事件
    safeAddEventListener('loginBtn', 'click', openAuthModal);
    safeAddEventListener('logoutBtn', 'click', logout);
    safeAddEventListener('closeAuthBtn', 'click', closeAuthModal);
    safeAddEventListener('loginTabBtn', 'click', () => switchAuthTab('login'));
    safeAddEventListener('registerTabBtn', 'click', () => switchAuthTab('register'));
    safeAddEventListener('submitLoginBtn', 'click', submitLogin);
    safeAddEventListener('submitRegisterBtn', 'click', submitRegister);
    safeAddEventListener('refreshCaptchaBtn', 'click', loadCaptcha);
    
    // 回车键提交
    const loginPasswordInput = document.getElementById('loginPassword');
    if (loginPasswordInput) {
        loginPasswordInput.addEventListener('keypress', function(e) {
            if (e.key === 'Enter') submitLogin();
        });
    }
    
    const registerPasswordConfirmInput = document.getElementById('registerPasswordConfirm');
    if (registerPasswordConfirmInput) {
        registerPasswordConfirmInput.addEventListener('keypress', function(e) {
            if (e.key === 'Enter') submitRegister();
        });
    }
    
    // 排序表头事件
    document.querySelectorAll('.sortable').forEach(th => {
        th.addEventListener('click', function() {
            const column = this.getAttribute('data-sort');
            if (!column) return;
            
            // 切换排序方向
            if (sortState.column === column) {
                sortState.direction = sortState.direction === 'asc' ? 'desc' : 'asc';
            } else {
                sortState.column = column;
                sortState.direction = 'asc';
            }
            
            // 更新表头样式
            document.querySelectorAll('.sortable').forEach(h => {
                h.classList.remove('sort-asc', 'sort-desc');
            });
            this.classList.add(`sort-${sortState.direction}`);
            
            // 重新显示（会自动应用排序）
            displayFunds(allFundsData);
        });
    });
    
    // 点击模态框外部关闭
    const settingsModal = document.getElementById('settingsModal');
    if (settingsModal) {
        settingsModal.addEventListener('click', function(e) {
            if (e.target === this) {
                closeSettings();
            }
        });
    }
    const donateModal = document.getElementById('donateModal');
    if (donateModal) {
        donateModal.addEventListener('click', function(e) {
            if (e.target === this) closeDonate();
        });
    }
    document.querySelectorAll('.donate-amount-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            document.querySelectorAll('.donate-amount-btn').forEach(b => b.classList.remove('active'));
            this.classList.add('active');
        });
    });
    document.querySelectorAll('.donate-pay-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            document.querySelectorAll('.donate-pay-btn').forEach(b => b.classList.remove('active'));
            this.classList.add('active');
        });
    });
    safeAddEventListener('confirmDonateBtn', 'click', openDonateQrModal);
    safeAddEventListener('closeDonateQrBtn', 'click', closeDonateQrModal);
    safeAddEventListener('closeDonateQrBtnFooter', 'click', closeDonateQrModal);
    const donateQrModal = document.getElementById('donateQrModal');
    if (donateQrModal) {
        donateQrModal.addEventListener('click', function(e) {
            if (e.target === this) closeDonateQrModal();
        });
    }
    
}

// 加载配置
async function loadConfig() {
    try {
        const response = await fetch('/api/config');
        const result = await response.json();
        if (result.success) {
            const config = result.data;
            updateInterval = config.update_interval || 60; // 默认60秒
            populateSettingsForm(config);
        }
    } catch (error) {
        log('加载配置失败: ' + error.message, 'error');
    }
}

// 填充设置表单
async function populateSettingsForm(config) {
    document.getElementById('buyCommission').value = (config.trade_fees.buy_commission * 100).toFixed(4);
    document.getElementById('sellCommission').value = (config.trade_fees.sell_commission * 100).toFixed(4);
    document.getElementById('subscribeFee').value = (config.trade_fees.subscribe_fee * 100).toFixed(2);
    document.getElementById('redeemFee').value = (config.trade_fees.redeem_fee * 100).toFixed(2);
    document.getElementById('stampTax').value = (config.trade_fees.stamp_tax * 100).toFixed(4);
    document.getElementById('minProfitRate').value = (config.arbitrage_threshold.min_profit_rate * 100).toFixed(2);
}

// 填充数据源配置
async function populateDataSources(dataSources, containerId = 'dataSourcesContainer') {
    const container = document.getElementById(containerId);
    if (!dataSources) {
        container.innerHTML = '<p class="placeholder">数据源配置未找到</p>';
        return;
    }
    
    // 获取数据源状态
    let statusData = {};
    try {
        const statusResponse = await fetch('/api/data-sources/status');
        const statusResult = await statusResponse.json();
        if (statusResult.success) {
            statusData = statusResult.data;
        }
    } catch (error) {
        console.error('获取数据源状态失败:', error);
    }
    
    let html = '';
    
    // 价格数据源
    html += '<div class="data-source-group"><h4>价格数据源</h4>';
    if (dataSources.price_sources) {
        const sorted = Object.entries(dataSources.price_sources).sort((a, b) => a[1].priority - b[1].priority);
        sorted.forEach(([key, source]) => {
            const status = statusData.price_sources?.[key] || {installed: true, available: true};
            html += createDataSourceRow(key, source, status, 'price_sources');
        });
    }
    html += '</div>';
    
    // 净值数据源
    html += '<div class="data-source-group"><h4>净值数据源</h4>';
    if (dataSources.nav_sources) {
        const sorted = Object.entries(dataSources.nav_sources).sort((a, b) => a[1].priority - b[1].priority);
        sorted.forEach(([key, source]) => {
            const status = statusData.nav_sources?.[key] || {installed: true, available: true};
            html += createDataSourceRow(key, source, status, 'nav_sources');
        });
    }
    html += '</div>';
    
    // 基金列表数据源
    html += '<div class="data-source-group"><h4>基金列表数据源</h4>';
    if (dataSources.fund_list_sources) {
        const sorted = Object.entries(dataSources.fund_list_sources).sort((a, b) => a[1].priority - b[1].priority);
        sorted.forEach(([key, source]) => {
            const status = statusData.fund_list_sources?.[key] || {installed: true, available: true};
            html += createDataSourceRow(key, source, status, 'fund_list_sources', source.token);
        });
    }
    html += '</div>';
    
    // 中文名称数据源
    html += '<div class="data-source-group"><h4>中文名称数据源</h4>';
    if (dataSources.name_sources) {
        const sorted = Object.entries(dataSources.name_sources).sort((a, b) => a[1].priority - b[1].priority);
        sorted.forEach(([key, source]) => {
            const status = statusData.name_sources?.[key] || {installed: true, available: true};
            html += createDataSourceRow(key, source, status, 'name_sources');
        });
    }
    html += '</div>';
    
    // 限购信息数据源
    html += '<div class="data-source-group"><h4>限购信息数据源</h4>';
    if (dataSources.purchase_limit_sources) {
        const sorted = Object.entries(dataSources.purchase_limit_sources).sort((a, b) => a[1].priority - b[1].priority);
        sorted.forEach(([key, source]) => {
            const status = statusData.purchase_limit_sources?.[key] || {installed: true, available: true};
            html += createDataSourceRow(key, source, status, 'purchase_limit_sources');
        });
    }
    html += '</div>';
    
    container.innerHTML = html;
}

// 创建数据源配置行
function createDataSourceRow(key, source, status, category, token = null) {
    const installedClass = status.installed ? 'installed' : 'not-installed';
    const statusText = status.installed ? (status.available ? '可用' : '不可用') : '未安装';
    
    let html = `<div class="data-source-item">
        <div class="data-source-header">
            <label class="switch">
                <input type="checkbox" class="data-source-enabled" 
                    data-category="${category}" 
                    data-key="${key}" 
                    ${source.enabled ? 'checked' : ''}
                    ${!status.installed ? 'disabled' : ''}>
                <span class="slider"></span>
            </label>
            <span class="data-source-name">${source.name}</span>
            <span class="data-source-status ${installedClass}">${statusText}</span>
            <span class="data-source-priority">优先级: 
                <input type="number" class="data-source-priority-input" 
                    data-category="${category}" 
                    data-key="${key}" 
                    value="${source.priority}" 
                    min="1" 
                    max="100" 
                    style="width: 50px; margin-left: 5px;">
            </span>
        </div>`;
    
    // 如果有关键配置（如token），显示输入框
    if (token !== null && key === 'tushare') {
        html += `<div class="data-source-config" style="margin-top: 5px; margin-left: 30px;">
            <label>Token:</label>
            <input type="text" class="data-source-token-input" 
                data-category="${category}" 
                data-key="${key}" 
                value="${token || ''}" 
                placeholder="Tushare Token"
                style="width: 400px; margin-left: 5px;">
        </div>`;
    }
    
    html += '</div>';
    return html;
}

// 加载基金列表
async function loadFunds() {
    
    const tbody = document.getElementById('fundsTableBody');
    
    
    if (!tbody) {
        console.error('基金表格元素不存在');
        return;
    }
    
    tbody.innerHTML = '<tr><td colspan="12" class="loading">正在加载数据...</td></tr>';
    
    try {
        // 优先使用快速API，一次性获取所有基金数据（秒级响应）
        tbody.innerHTML = '<tr><td colspan="12" class="loading">正在加载数据...</td></tr>';
        
        const startTime = Date.now();
        const response = await fetch('/api/funds/all');
        const result = await response.json();
        const loadTime = Date.now() - startTime;
        
        
        if (result.success && result.data && result.data.length > 0) {
            // 快速API成功，直接显示数据（申购状态等信息直接来自数据库，秒级加载）
            displayFunds(result.data);
            log(`成功加载 ${result.data.length} 只基金数据 (耗时 ${(loadTime/1000).toFixed(2)}秒)`, 'success');
            updateLastUpdateTime();
            // 申购状态依赖后台定时刷新数据库，这里不再额外调用异步刷新接口，避免页面加载时重复命中第三方数据源
            return;
        }
        
        // 如果快速API失败或返回空数据，回退到原来的分批加载方式
        console.warn('快速API未返回数据，回退到分批加载方式');
        
        // 获取基金代码列表
        const fundsResponse = await fetch('/api/funds');
        const fundsResult = await fundsResponse.json();
        
        if (!fundsResult.success) {
            throw new Error('获取基金列表失败: ' + (fundsResult.message || '未知错误'));
        }
        
        const fundCodes = Object.keys(fundsResult.funds || {});
        
        if (fundCodes.length === 0) {
            throw new Error('未能加载任何基金数据，请检查SSE数据源或联系管理员');
        }
        
        // 分批加载基金信息（每批50只，避免超时）
        const batchSize = 50;
        const batches = [];
        for (let i = 0; i < fundCodes.length; i += batchSize) {
            batches.push(fundCodes.slice(i, i + batchSize));
        }
        
        tbody.innerHTML = `<tr><td colspan="12" class="loading">正在加载数据... (0/${fundCodes.length})</td></tr>`;
        
        let allResults = [];
        let processedCount = 0;
        
        // 逐批加载
        for (let batchIndex = 0; batchIndex < batches.length; batchIndex++) {
            const batch = batches[batchIndex];
            
            // 更新加载提示
            tbody.innerHTML = `<tr><td colspan="12" class="loading">正在加载数据... (${processedCount}/${fundCodes.length}) - 批次 ${batchIndex + 1}/${batches.length}</td></tr>`;
            
            try {
                // 创建超时控制器（每批120秒）
                const controller = new AbortController();
                const timeoutId = setTimeout(() => {
                    controller.abort();
                }, 120000); // 每批120秒超时
                
                const response = await fetch('/api/funds/batch', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({ codes: batch }),
                    signal: controller.signal
                });
                
                clearTimeout(timeoutId);
                
                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
                }
                
                const batchResult = await response.json();
                
                if (batchResult.success && batchResult.data) {
                    allResults = allResults.concat(batchResult.data);
                    processedCount += batch.length;
                    
                    if (batchResult.data.length < batch.length) {
                        log(`批次 ${batchIndex + 1}: 请求 ${batch.length} 只，实际返回 ${batchResult.data.length} 只`, 'warning');
                    }
                } else {
                    log(`批次 ${batchIndex + 1} 加载失败: ${batchResult.message || '未知错误'}`, 'warning');
                }
            } catch (fetchError) {
                if (fetchError.name === 'AbortError') {
                    log(`批次 ${batchIndex + 1} 超时，跳过`, 'warning');
                } else {
                    log(`批次 ${batchIndex + 1} 加载失败: ${fetchError.message}`, 'error');
                }
                // 继续处理下一批，不中断
            }
        }
        
        if (allResults.length > 0) {
            displayFunds(allResults);
            if (allResults.length < fundCodes.length) {
                const missing = fundCodes.length - allResults.length;
                const percentage = ((allResults.length / fundCodes.length) * 100).toFixed(1);
                log(`部分加载: ${allResults.length}/${fundCodes.length} 只基金数据 (${percentage}%，缺失 ${missing} 只)`, 'warning');
            } else {
                log(`成功加载 ${allResults.length}/${fundCodes.length} 只基金数据`, 'success');
            }
            updateLastUpdateTime();
            // 回退模式下，同样只依赖数据库中已有的申购状态，不在前端主动触发批量刷新
        } else {
            throw new Error('未能加载任何基金数据');
        }
        
        return; // 提前返回，避免执行下面的代码
    } catch (error) {
        
        tbody.innerHTML = `<tr><td colspan="12" class="loading" style="color: #f44336;">加载失败: ${error.message}</td></tr>`;
        log('加载基金数据失败: ' + error.message, 'error');
    }
}

// 存储所有基金数据（用于筛选和排序）
let allFundsData = [];
// 排序状态：{column: 'fund_code', direction: 'asc'|'desc'}
let sortState = { column: null, direction: 'asc' };
// 搜索关键词
let searchKeyword = '';

// 显示基金列表
function displayFunds(funds) {
    allFundsData = funds; // 保存所有基金数据
    
    const tbody = document.getElementById('fundsTableBody');
    
    if (funds.length === 0) {
        tbody.innerHTML = '<tr><td colspan="12" class="loading">暂无数据</td></tr>';
        return;
    }
    
    // 应用自选筛选
    let filteredFunds = funds;
    if (showFavoritesOnly) {
        filteredFunds = funds.filter(fund => favoriteFunds.has(fund.fund_code));
    }

    // 应用退市过滤
    if (hideDelistedFunds) {
        filteredFunds = filteredFunds.filter(fund => !fund.is_exchange_delisted);
    }

    // 应用搜索过滤
    if (searchKeyword.trim()) {
        const keyword = searchKeyword.trim().toUpperCase();
        filteredFunds = filteredFunds.filter(fund => {
            const code = (fund.fund_code || '').toUpperCase();
            const name = (fund.fund_name || '').toUpperCase();
            return code.includes(keyword) || name.includes(keyword);
        });
    }
    
    // 更新统计信息
    updateStats(filteredFunds);
    
    // 应用排序
    let sortedFunds = filteredFunds;
    if (sortState.column) {
        sortedFunds = sortFunds(filteredFunds, sortState.column, sortState.direction);
    }
    
    tbody.innerHTML = sortedFunds.map(fund => {
        const isOpportunity = fund.has_opportunity;
        const isDelisted = fund.is_exchange_delisted;
        const rowClass = isDelisted ? 'fund-delisted' : (isOpportunity ? 'opportunity' : '');
        const statusClass = isDelisted ? 'status-delisted' : (isOpportunity ? 'status-opportunity' : 'status-none');
        const statusText = isDelisted ? '场内退市' : (isOpportunity ? '有机会' : '无机会');
        const typeClass = fund.arbitrage_type === '溢价套利' ? 'type-premium' : 'type-discount';
        const profitClass = fund.profit_rate >= 0 ? 'positive' : 'negative';
        const profitSign = fund.profit_rate >= 0 ? '+' : '';
        
        // 格式化申购状态
        let purchaseLimitDisplay = '<span style="color: #4CAF50;">开放申购</span>';
        if (fund.purchase_limit) {
            const purchaseStatus = fund.purchase_limit.purchase_status;
            if (purchaseStatus === '暂停申购') {
                purchaseLimitDisplay = '<span style="color: #f44336;">暂停申购</span>';
            } else if (purchaseStatus === '限购' && fund.purchase_limit.limit_amount) {
                const amount = fund.purchase_limit.limit_amount;
                const display = amount >= 10000 ? 
                    (amount / 10000).toFixed(1) + '万' : 
                    amount.toFixed(0) + '元';
                purchaseLimitDisplay = `<span style="color: #ff9800;">限购 ${display}</span>`;
            } else if (purchaseStatus === '开放申购') {
                purchaseLimitDisplay = '<span style="color: #4CAF50;">开放申购</span>';
            } else if (fund.purchase_limit.is_limited && fund.purchase_limit.limit_amount) {
                // 兼容旧数据：如果没有 purchase_status 字段，使用旧的逻辑
                const amount = fund.purchase_limit.limit_amount;
                const display = amount >= 10000 ? 
                    (amount / 10000).toFixed(1) + '万' : 
                    amount.toFixed(0) + '元';
                purchaseLimitDisplay = `<span style="color: #ff9800;">限购 ${display}</span>`;
            }
        }
        
        const isFavorite = favoriteFunds.has(fund.fund_code);
        const starIcon = isFavorite ? '⭐' : '☆';
        const starClass = isFavorite ? 'favorite-star active' : 'favorite-star';
        
        return `
            <tr class="${rowClass}" data-code="${fund.fund_code}">
                <td class="fund-code-name"><strong>${fund.fund_code}</strong>${isDelisted ? ' <span class="badge-delisted" title="场内K线已停更，该基金可能已退出交易所上市">退市</span>' : ''}<br><span class="fund-name-text">${fund.fund_name || '--'}</span></td>
                <td>${fund.price > 0 ? fund.price.toFixed(4) : '--'}</td>
                <td class="${fund.change_pct >= 0 ? 'positive' : 'negative'}">${fund.change_pct !== undefined && fund.change_pct !== null ? (fund.change_pct >= 0 ? '+' : '') + fund.change_pct.toFixed(2) + '%' : '--'}</td>
                <td>${fund.nav > 0 ? fund.nav.toFixed(4) : '--'}</td>
                <td style="font-size: 12px;">${fund.nav_date || '--'}</td>
                <td class="${fund.price_diff_pct >= 0 ? 'positive' : 'negative'}">${fund.price_diff_pct >= 0 ? '+' : ''}${fund.price_diff_pct.toFixed(2)}%</td>
                <td><span class="arbitrage-type ${typeClass}">${fund.arbitrage_type}</span></td>
                <td class="profit-rate ${profitClass}">${profitSign}${fund.profit_rate.toFixed(2)}%</td>
                <td style="font-size: 12px; white-space: nowrap;">${purchaseLimitDisplay}</td>
                <td><span class="status-badge ${statusClass}">${statusText}</span></td>
                <td>
                    <button class="btn btn-small btn-primary" onclick="openRecordArbitrageModal('${fund.fund_code}', '${(fund.fund_name || '').replace(/'/g, "\\'")}', '${fund.arbitrage_type}', ${fund.price}, ${fund.nav})" title="记录套利交易">记录</button>
                </td>
                <td>
                    <span class="${starClass}" data-code="${fund.fund_code}" style="cursor: pointer; font-size: 18px; user-select: none;" title="${isFavorite ? '取消自选' : '加入自选'}">${starIcon}</span>
                </td>
            </tr>
        `;
    }).join('');
    
    
    // 添加自选星标点击事件
    tbody.querySelectorAll('.favorite-star').forEach(star => {
        star.addEventListener('click', function(e) {
            e.stopPropagation(); // 阻止事件冒泡
            const code = this.getAttribute('data-code');
            toggleFavorite(code);
        });
    });
}

// 排序基金
function sortFunds(funds, column, direction) {
    const sorted = [...funds].sort((a, b) => {
        let aVal, bVal;
        
        switch(column) {
            case 'fund_code':
                aVal = a.fund_code || '';
                bVal = b.fund_code || '';
                return aVal.localeCompare(bVal);
            case 'fund_name':
                aVal = (a.fund_name || '').toLowerCase();
                bVal = (b.fund_name || '').toLowerCase();
                return aVal.localeCompare(bVal);
            case 'fund_type':
                aVal = getFundType(a);
                bVal = getFundType(b);
                return aVal.localeCompare(bVal);
            case 'price':
                aVal = a.price || 0;
                bVal = b.price || 0;
                return aVal - bVal;
            case 'change_pct':
                aVal = a.change_pct || 0;
                bVal = b.change_pct || 0;
                return aVal - bVal;
            case 'nav':
                aVal = a.nav || 0;
                bVal = b.nav || 0;
                return aVal - bVal;
            case 'price_diff_pct':
                aVal = a.price_diff_pct || 0;
                bVal = b.price_diff_pct || 0;
                return aVal - bVal;
            case 'arbitrage_type':
                aVal = (a.arbitrage_type || '').toLowerCase();
                bVal = (b.arbitrage_type || '').toLowerCase();
                return aVal.localeCompare(bVal);
            case 'profit_rate':
                aVal = a.profit_rate || 0;
                bVal = b.profit_rate || 0;
                return aVal - bVal;
            case 'purchase_limit':
                // 排序逻辑：开放申购排在前面（值为-1），限购的按限购金额排序，暂停申购排在最后（值为999999999）
                const getPurchaseLimitSortValue = (fund) => {
                    if (!fund.purchase_limit) return -1;
                    const status = fund.purchase_limit.purchase_status;
                    if (status === '暂停申购') return 999999999;
                    if (status === '开放申购') return -1;
                    if (status === '限购' && fund.purchase_limit.limit_amount) {
                        return fund.purchase_limit.limit_amount;
                    }
                    // 兼容旧数据
                    if (fund.purchase_limit.is_limited && fund.purchase_limit.limit_amount) {
                        return fund.purchase_limit.limit_amount;
                    }
                    return -1;
                };
                aVal = getPurchaseLimitSortValue(a);
                bVal = getPurchaseLimitSortValue(b);
                return aVal - bVal;
            case 'has_opportunity':
                aVal = a.has_opportunity ? 1 : 0;
                bVal = b.has_opportunity ? 1 : 0;
                return aVal - bVal;
            default:
                return 0;
        }
    });
    
    return direction === 'desc' ? sorted.reverse() : sorted;
}

// 更新统计信息
function updateStats(funds) {
    document.getElementById('fundCount').textContent = funds.length;

    const opportunities = funds.filter(f => f.has_opportunity).length;
    document.getElementById('opportunityCount').textContent = opportunities;

    // 更新自选基金数量
    const favoriteCount = funds.filter(f => favoriteFunds.has(f.fund_code)).length;
    document.getElementById('favoriteCount').textContent = favoriteCount;

    // 更新退市基金数量
    const delistedEl = document.getElementById('delistedCount');
    if (delistedEl) {
        const delistedCount = allFundsData.filter(f => f.is_exchange_delisted).length;
        delistedEl.textContent = delistedCount;
    }

    const now = new Date();
    document.getElementById('lastUpdate').textContent = now.toLocaleTimeString('zh-CN');
}

// 更新最后更新时间
function updateLastUpdateTime() {
    const now = new Date();
    const timeStr = now.toLocaleTimeString('zh-CN');
    document.getElementById('lastUpdate').textContent = timeStr;
}

// 切换自动刷新
function toggleAutoRefresh() {
    const btn = document.getElementById('autoRefreshBtn');
    
    if (autoRefreshInterval) {
        clearInterval(autoRefreshInterval);
        autoRefreshInterval = null;
        btn.textContent = '自动刷新';
        btn.classList.remove('btn-primary');
        btn.classList.add('btn-secondary');
        log('已停止自动刷新', 'info');
    } else {
        autoRefreshInterval = setInterval(loadFunds, updateInterval * 1000);
        btn.textContent = '停止刷新';
        btn.classList.remove('btn-secondary');
        btn.classList.add('btn-primary');
        log(`已开启自动刷新 (间隔: ${updateInterval}秒)`, 'success');
    }
}

// 打开设置
function openSettings() {
    if (!checkLogin()) {
        requireLogin();
        return;
    }
    document.getElementById('settingsModal').classList.add('active');
}

// 关闭设置
function closeSettings() {
    document.getElementById('settingsModal').classList.remove('active');
}

// 打开捐助
function openDonate() {
    document.querySelectorAll('.donate-amount-btn').forEach(btn => {
        btn.classList.toggle('active', btn.getAttribute('data-amount') === '20');
    });
    document.querySelectorAll('.donate-pay-btn').forEach(btn => {
        btn.classList.toggle('active', btn.getAttribute('data-pay') === 'wechat');
    });
    document.getElementById('donateModal').classList.add('active');
}

// 关闭捐助
function closeDonate() {
    document.getElementById('donateModal').classList.remove('active');
}

// 获取当前选中的捐助金额
function getSelectedDonateAmount() {
    const btn = document.querySelector('.donate-amount-btn.active');
    return btn ? btn.getAttribute('data-amount') : '20';
}

// 获取当前选中的支付方式
function getSelectedDonatePay() {
    const btn = document.querySelector('.donate-pay-btn.active');
    return btn ? btn.getAttribute('data-pay') : 'wechat';
}

// 确认支付：弹出二维码
function openDonateQrModal() {
    const amount = getSelectedDonateAmount();
    const pay = getSelectedDonatePay();
    document.getElementById('donateQrAmount').innerHTML = '支付金额：<strong>' + amount + '</strong> 元';
    document.getElementById('donateQrMethod').textContent = '支付方式：' + (pay === 'wechat' ? '微信' : '支付宝');
    const imgWechat = document.getElementById('donateQrImgWechat');
    const imgAlipay = document.getElementById('donateQrImgAlipay');
    const placeholder = document.getElementById('donateQrPlaceholder');
    if (imgWechat && imgAlipay && placeholder) {
        imgWechat.style.display = pay === 'wechat' ? 'block' : 'none';
        imgAlipay.style.display = pay === 'alipay' ? 'block' : 'none';
        placeholder.style.display = 'none';
        const showImg = pay === 'wechat' ? imgWechat : imgAlipay;
        showImg.onerror = function() {
            showImg.style.display = 'none';
            placeholder.style.display = 'block';
        };
    }
    document.getElementById('donateQrModal').classList.add('active');
}

// 关闭捐助二维码弹窗
function closeDonateQrModal() {
    document.getElementById('donateQrModal').classList.remove('active');
}

// 保存设置
async function saveSettings() {
    const tradeFees = {
        buy_commission: parseFloat(document.getElementById('buyCommission').value) / 100,
        sell_commission: parseFloat(document.getElementById('sellCommission').value) / 100,
        subscribe_fee: parseFloat(document.getElementById('subscribeFee').value) / 100,
        redeem_fee: parseFloat(document.getElementById('redeemFee').value) / 100,
        stamp_tax: parseFloat(document.getElementById('stampTax').value) / 100
    };
    
    const arbitrageThreshold = {
        min_profit_rate: parseFloat(document.getElementById('minProfitRate').value) / 100,
        // 保留最小溢价率字段以兼容后端，但使用默认值（不在UI中显示）
        min_price_diff: 0.01
    };
    
    try {
        const response = await fetch('/api/config', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                trade_fees: tradeFees,
                arbitrage_threshold: arbitrageThreshold
            })
        });
        
        const result = await response.json();
        
        if (result.success) {
            log('设置保存成功', 'success');
            closeSettings();
            
            // 保存设置后立即刷新基金列表，以应用新的阈值和费率
            log('正在刷新基金列表以应用新设置...', 'info');
            await loadFunds();
            
            // 如果正在自动刷新，重新设置间隔
            if (autoRefreshInterval) {
                toggleAutoRefresh();
                toggleAutoRefresh();
            }
        } else {
            // 检查是否是登录问题
            if (result.requires_login || response.status === 401) {
                alert('请先登录: ' + (result.message || '未登录'));
                closeSettings();
                if (typeof openAuthModal === 'function') {
                    openAuthModal();
                }
            } else {
                throw new Error(result.message);
            }
        }
    } catch (error) {
        log('保存设置失败: ' + error.message, 'error');
        alert('保存设置失败: ' + error.message);
    }
}

// ==================== 数据源配置相关函数（仅管理员） ====================

// 打开数据源配置
async function openDataSourceConfig() {
    if (!checkLogin()) {
        requireLogin();
        return;
    }
    
    // 检查是否为管理员
    if (!currentUser || currentUser.role !== 'admin') {
        alert('此功能需要管理员权限');
        return;
    }
    
    // 加载数据源配置
    try {
        const response = await fetch('/api/data-sources/config');
        const result = await response.json();
        if (result.success) {
            const config = result.data;
            document.getElementById('dataSourceUpdateInterval').value = config.update_interval;
            
            // 加载数据时效性配置
            const dataFreshness = config.data_sources?.data_freshness || {};
            document.getElementById('priceNavMaxAge').value = dataFreshness.price_nav_max_age_seconds || 300;
            document.getElementById('purchaseLimitMaxAge').value = dataFreshness.purchase_limit_max_age_seconds || 600;
            document.getElementById('purchaseLimitUpdateInterval').value = dataFreshness.purchase_limit_update_interval || 600;
            
            await populateDataSources(config.data_sources, 'dataSourceConfigContainer');
        }
    } catch (error) {
        log('加载数据源配置失败: ' + error.message, 'error');
    }
    
    document.getElementById('dataSourceConfigModal').classList.add('active');
}

// 关闭数据源配置
function closeDataSourceConfig() {
    document.getElementById('dataSourceConfigModal').classList.remove('active');
}

// 保存数据源配置
async function saveDataSourceConfig() {
    if (!checkLogin()) {
        requireLogin();
        return;
    }
    
    // 检查是否为管理员
    if (!currentUser || currentUser.role !== 'admin') {
        alert('此功能需要管理员权限');
        return;
    }
    
    const updateInterval = parseInt(document.getElementById('dataSourceUpdateInterval').value);
    const priceNavMaxAge = parseInt(document.getElementById('priceNavMaxAge').value);
    const purchaseLimitMaxAge = parseInt(document.getElementById('purchaseLimitMaxAge').value);
    const purchaseLimitUpdateInterval = parseInt(document.getElementById('purchaseLimitUpdateInterval').value);
    
    // 收集数据源配置
    const dataSources = {
        update_interval: updateInterval,
        data_freshness: {
            price_nav_max_age_seconds: priceNavMaxAge,
            purchase_limit_max_age_seconds: purchaseLimitMaxAge,
            purchase_limit_update_interval: purchaseLimitUpdateInterval
        },
        price_sources: {},
        nav_sources: {},
        fund_list_sources: {},
        name_sources: {},
        purchase_limit_sources: {}
    };
    
    // 收集所有数据源配置（从数据源配置模态框）
    document.querySelectorAll('#dataSourceConfigModal .data-source-enabled').forEach(checkbox => {
        const category = checkbox.getAttribute('data-category');
        const key = checkbox.getAttribute('data-key');
        const priorityInput = document.querySelector(`#dataSourceConfigModal .data-source-priority-input[data-category="${category}"][data-key="${key}"]`);
        const priority = priorityInput ? parseInt(priorityInput.value) : 1;
        
        if (!dataSources[category]) {
            dataSources[category] = {};
        }
        
        dataSources[category][key] = {
            enabled: checkbox.checked,
            priority: priority
        };
        
        // 如果有token输入框，也收集
        const tokenInput = document.querySelector(`#dataSourceConfigModal .data-source-token-input[data-category="${category}"][data-key="${key}"]`);
        if (tokenInput) {
            dataSources[category][key].token = tokenInput.value;
        }
    });
    
    try {
        const response = await fetch('/api/data-sources/config', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                data_sources: dataSources
            })
        });
        
        const result = await response.json();
        
        if (result.success) {
            log('数据源配置保存成功', 'success');
            
            // 如果更新了数据时效性配置，通知后台更新器重新加载配置
            if (result.background_updater_restarted) {
                log('后台更新器已重新加载配置', 'info');
            }
            
            closeDataSourceConfig();
            
            // 如果正在自动刷新，重新设置间隔
            if (autoRefreshInterval) {
                toggleAutoRefresh();
                toggleAutoRefresh();
            }
        } else {
            // 检查是否是权限问题
            if (result.requires_admin || response.status === 403) {
                alert('需要管理员权限: ' + (result.message || '权限不足'));
                closeDataSourceConfig();
            } else {
                throw new Error(result.message);
            }
        }
    } catch (error) {
        log('保存数据源配置失败: ' + error.message, 'error');
        alert('保存数据源配置失败: ' + error.message);
    }
}

// ==================== 异步更新限购 ====================

// 异步更新限购信息（不阻塞主流程）
async function updatePurchaseLimitsAsync(fundCodes) {
    if (!fundCodes || fundCodes.length === 0) return;
    
    try {
        
        // 分批获取限购信息（每批100只）
        const batchSize = 100;
        for (let i = 0; i < fundCodes.length; i += batchSize) {
            const batch = fundCodes.slice(i, i + batchSize);
            try {
                const response = await fetch('/api/funds/purchase-limits', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ codes: batch })
                });
                const result = await response.json();
                
                if (result.success && result.limits) {
                    // 更新已显示的基金数据
                    if (typeof allFundsData !== 'undefined' && allFundsData.length > 0) {
                        const fundsToUpdate = allFundsData.filter(f => batch.includes(f.fund_code));
                        fundsToUpdate.forEach(fund => {
                            if (result.limits[fund.fund_code]) {
                                fund.purchase_limit = result.limits[fund.fund_code];
                            }
                        });
                        
                        // 重新显示更新后的数据（只更新一次，避免频繁刷新）
                        if (i === 0 || i + batchSize >= fundCodes.length) {
                            displayFunds(allFundsData);
                        }
                    }
                }
            } catch (error) {
                // 单批失败不影响其他批次
                console.error(`批量 ${Math.floor(i / batchSize) + 1} 限购信息获取失败:`, error);
            }
        }
        
        // 最终更新显示
        if (typeof allFundsData !== 'undefined' && allFundsData.length > 0) {
            displayFunds(allFundsData);
        }
        log(`限购信息更新完成`, 'info');
    } catch (error) {
        console.error('异步获取限购信息失败:', error);
        // 不显示错误给用户，因为这是后台操作
    }
}

// 日志功能
function log(message, type = 'info') {
    const logContent = document.getElementById('logContent');
    const time = new Date().toLocaleTimeString('zh-CN');
    const entry = document.createElement('div');
    entry.className = `log-entry ${type}`;
    entry.textContent = `[${time}] ${message}`;
    logContent.appendChild(entry);
    logContent.scrollTop = logContent.scrollHeight;
}

// 清空日志
function clearLog() {
    document.getElementById('logContent').innerHTML = '';
}

// 发现LOF基金
async function discoverFunds() {
    const btn = document.getElementById('discoverBtn');
    const originalText = btn.textContent;
    btn.disabled = true;
    btn.textContent = '发现中...';
    
    try {
        // 先尝试下载SSE数据（如果Excel文件不存在）
        try {
            log('正在尝试下载SSE数据...', 'info');
            const downloadResponse = await fetch('/api/sse/download', { method: 'POST' });
            const downloadResult = await downloadResponse.json();
            if (downloadResult.success) {
                log('SSE数据下载成功', 'success');
            } else {
                log('SSE数据下载失败（可能已存在或Selenium未安装）: ' + downloadResult.message, 'warning');
            }
        } catch (downloadError) {
            log('SSE数据下载失败: ' + downloadError.message, 'warning');
        }
        
        // 然后发现基金
        const response = await fetch('/api/funds/discover');
        const result = await response.json();
        
        if (result.success) {
            if (result.funds && Object.keys(result.funds).length > 0) {
                // 更新基金列表
                log(`发现 ${result.count} 只LOF基金，总计 ${result.total_count} 只`, 'success');
                
                // 重新加载基金数据
                await loadFunds();
                
                alert(`成功发现 ${result.count} 只LOF基金！已更新基金列表。`);
            } else {
                log('未发现LOF基金', 'warning');
                alert('未发现LOF基金。\n\n提示：如果只看到50开头的基金，可能是SSE Excel文件不存在。\n请手动下载Excel文件到data文件夹，或等待自动下载任务执行。');
            }
        } else {
            throw new Error(result.message || '发现基金失败');
        }
    } catch (error) {
        log('发现基金失败: ' + error.message, 'error');
        alert('发现基金失败: ' + error.message + '\n\n提示：如果只看到50开头的基金，可能是SSE Excel文件不存在。\n请手动下载Excel文件到data文件夹，或等待自动下载任务执行。');
    } finally {
        btn.disabled = false;
        btn.textContent = originalText;
    }
}

// 加载自选基金列表
async function loadFavoriteFunds() {
    if (!checkLogin()) {
        favoriteFunds = new Set();
        return;
    }
    
    try {
        const response = await fetch('/api/user/favorites');
        const result = await response.json();
        if (result.success && result.favorites) {
            favoriteFunds = new Set(result.favorites);
        } else {
            favoriteFunds = new Set();
        }
    } catch (error) {
        console.error('加载自选基金失败:', error);
        favoriteFunds = new Set();
    }
}

// 保存自选基金列表
async function saveFavoriteFunds() {
    if (!checkLogin()) {
        return;
    }
    
    try {
        const codes = Array.from(favoriteFunds);
        const response = await fetch('/api/user/favorites', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ favorites: codes })
        });
        const result = await response.json();
        if (!result.success) {
            console.error('保存自选基金失败:', result.message);
        }
    } catch (error) {
        console.error('保存自选基金失败:', error);
    }
}

// 检查是否已登录
function checkLogin() {
    return currentUser !== null;
}

// 提示需要登录
function requireLogin() {
    // 直接弹出登录窗口，不显示alert
    openAuthModal();
}

// 切换自选状态
function toggleFavorite(fundCode) {
    if (!checkLogin()) {
        requireLogin();
        return;
    }
    
    if (favoriteFunds.has(fundCode)) {
        favoriteFunds.delete(fundCode);
    } else {
        favoriteFunds.add(fundCode);
    }
    saveFavoriteFunds();
    displayFunds(allFundsData); // 重新显示以更新星标状态
}

// 切换自选筛选
function toggleFavoritesFilter() {
    if (!checkLogin()) {
        requireLogin();
        return;
    }
    
    showFavoritesOnly = !showFavoritesOnly;
    const btn = document.getElementById('filterFavoritesBtn');
    if (showFavoritesOnly) {
        btn.textContent = '全部';
        btn.classList.add('active');
    } else {
        btn.textContent = '自选';
        btn.classList.remove('active');
    }
    displayFunds(allFundsData); // 重新显示以应用筛选
}

// 切换退市基金隐藏
function toggleDelistedFilter() {
    hideDelistedFunds = !hideDelistedFunds;
    const btn = document.getElementById('hideDelistedBtn');
    if (hideDelistedFunds) {
        btn.textContent = '显示退市';
        btn.classList.add('active');
    } else {
        btn.textContent = '隐藏退市';
        btn.classList.remove('active');
    }
    displayFunds(allFundsData);
}

// ==================== 用户认证相关函数 ====================

// 检查登录状态
async function checkAuthStatus() {
    try {
        const response = await fetch('/api/auth/current');
        const result = await response.json();
        
        if (result.success && result.user) {
            currentUser = result.user;
            updateUserUI(true);
            // 登录后加载用户的自选基金
            await loadFavoriteFunds();
            // 登录后重新加载用户设置
            await loadConfig();
            // 重新显示基金列表以更新自选状态
            if (allFundsData && allFundsData.length > 0) {
                displayFunds(allFundsData);
            }
        } else {
            currentUser = null;
            favoriteFunds = new Set(); // 未登录时清空自选
            updateUserUI(false);
            // 停止通知检查
            if (notificationCheckInterval) {
                clearInterval(notificationCheckInterval);
                notificationCheckInterval = null;
            }
        }
    } catch (error) {
        console.error('检查登录状态失败:', error);
        currentUser = null;
        updateUserUI(false);
        // 停止通知检查
        if (notificationCheckInterval) {
            clearInterval(notificationCheckInterval);
            notificationCheckInterval = null;
        }
    }
}

// 更新用户UI显示
function updateUserUI(isLoggedIn) {
    const userInfo = document.getElementById('userInfo');
    const loginBtn = document.getElementById('loginBtn');
    const usernameDisplay = document.getElementById('usernameDisplay');
    
    if (isLoggedIn && currentUser) {
        userInfo.style.display = 'flex';
        loginBtn.style.display = 'none';
        usernameDisplay.textContent = `欢迎, ${currentUser.username}`;
        // 加载未读通知数量
        loadUnreadNotificationCount();
    } else {
        userInfo.style.display = 'none';
        loginBtn.style.display = 'block';
        // 隐藏通知容器
        const notificationContainer = document.getElementById('notificationContainer');
        if (notificationContainer) {
            notificationContainer.style.display = 'none';
        }
    }
    
    // 更新按钮状态（根据登录状态启用/禁用）
    updateFeatureButtons(isLoggedIn);
}

// 更新功能按钮状态
function updateFeatureButtons(isLoggedIn) {
    const filterFavoritesBtn = document.getElementById('filterFavoritesBtn');
    const arbitrageRecordsBtn = document.getElementById('arbitrageRecordsBtn');
    const settingsBtn = document.getElementById('settingsBtn');
    const dataSourceConfigBtn = document.getElementById('dataSourceConfigBtn');
    
    // 不禁用按钮，让用户点击时直接弹出登录窗口
    // 只更新提示信息
    if (filterFavoritesBtn) {
        if (!isLoggedIn) {
            filterFavoritesBtn.title = '需要登录（点击将弹出登录窗口）';
        } else {
            filterFavoritesBtn.title = '';
        }
    }
    
    if (arbitrageRecordsBtn) {
        if (!isLoggedIn) {
            arbitrageRecordsBtn.title = '需要登录（点击将弹出登录窗口）';
        } else {
            arbitrageRecordsBtn.title = '';
        }
    }
    
    if (settingsBtn) {
        if (!isLoggedIn) {
            settingsBtn.title = '需要登录（点击将弹出登录窗口）';
        } else {
            settingsBtn.title = '';
        }
    }
    
    // 数据源配置按钮：只有管理员可见
    if (dataSourceConfigBtn) {
        const isAdmin = isLoggedIn && currentUser && currentUser.role === 'admin';
        if (isAdmin) {
            dataSourceConfigBtn.style.display = 'inline-block';
            dataSourceConfigBtn.title = '';
        } else {
            dataSourceConfigBtn.style.display = 'none';
        }
    }
    
    // 所有套利记录按钮：只有管理员可见
    const adminArbitrageRecordsBtn = document.getElementById('adminArbitrageRecordsBtn');
    if (adminArbitrageRecordsBtn) {
        const isAdmin = isLoggedIn && currentUser && currentUser.role === 'admin';
        if (isAdmin) {
            adminArbitrageRecordsBtn.style.display = 'inline-block';
            adminArbitrageRecordsBtn.title = '';
        } else {
            adminArbitrageRecordsBtn.style.display = 'none';
        }
    }
}

// 打开认证模态框
function openAuthModal() {
    
    const modal = document.getElementById('authModal');
    
    if (modal) {
        // 嵌入主界面后，登录由主界面统一管理
        alert('请在主界面（左上角）登录后使用此功能');
        return;
        modal.classList.add('active');
        switchAuthTab('login');
    } else {
        console.error('登录模态框元素不存在');
        alert('登录模态框元素不存在');
    }
}

// 关闭认证模态框
function closeAuthModal() {
    const modal = document.getElementById('authModal');
    modal.classList.remove('active');
    // 清空表单
    document.getElementById('loginUsername').value = '';
    document.getElementById('loginPassword').value = '';
    document.getElementById('registerUsername').value = '';
    document.getElementById('registerPassword').value = '';
    document.getElementById('registerPasswordConfirm').value = '';
    document.getElementById('registerEmail').value = '';
    document.getElementById('loginError').style.display = 'none';
    document.getElementById('registerError').style.display = 'none';
    
    // 清除验证提示
    const usernameError = document.getElementById('usernameError');
    const usernameSuccess = document.getElementById('usernameSuccess');
    const usernameHint = document.getElementById('usernameHint');
    const emailError = document.getElementById('emailError');
    const emailSuccess = document.getElementById('emailSuccess');
    const emailHint = document.getElementById('emailHint');
    
    if (usernameError) usernameError.style.display = 'none';
    if (usernameSuccess) usernameSuccess.style.display = 'none';
    if (usernameHint) usernameHint.style.display = 'block';
    if (emailError) emailError.style.display = 'none';
    if (emailSuccess) emailSuccess.style.display = 'none';
    if (emailHint) emailHint.style.display = 'block';
}

// 切换登录/注册标签
function switchAuthTab(tab) {
    const loginForm = document.getElementById('loginForm');
    const registerForm = document.getElementById('registerForm');
    const loginTabBtn = document.getElementById('loginTabBtn');
    const registerTabBtn = document.getElementById('registerTabBtn');
    const authModalTitle = document.getElementById('authModalTitle');
    
    if (tab === 'login') {
        loginForm.style.display = 'block';
        registerForm.style.display = 'none';
        loginTabBtn.classList.add('active');
        loginTabBtn.style.borderBottom = '2px solid #007bff';
        loginTabBtn.style.color = '#333';
        registerTabBtn.classList.remove('active');
        registerTabBtn.style.borderBottom = 'none';
        registerTabBtn.style.color = '#666';
        authModalTitle.textContent = '登录';
    } else {
        // 切换到注册标签时加载验证码
        loadCaptcha();
        loginForm.style.display = 'none';
        registerForm.style.display = 'block';
        registerTabBtn.classList.add('active');
        registerTabBtn.style.borderBottom = '2px solid #007bff';
        registerTabBtn.style.color = '#333';
        loginTabBtn.classList.remove('active');
        loginTabBtn.style.borderBottom = 'none';
        loginTabBtn.style.color = '#666';
        authModalTitle.textContent = '注册';
    }
    
    // 清空错误信息
    document.getElementById('loginError').style.display = 'none';
    document.getElementById('registerError').style.display = 'none';
}

// 提交登录
async function submitLogin() {
    
    const usernameInput = document.getElementById('loginUsername');
    const passwordInput = document.getElementById('loginPassword');
    const errorDiv = document.getElementById('loginError');
    
    
    if (!usernameInput || !passwordInput || !errorDiv) {
        alert('登录表单元素不存在');
        return;
    }
    
    const username = usernameInput.value.trim();
    const password = passwordInput.value;
    
    if (!username || !password) {
        errorDiv.textContent = '请输入用户名和密码';
        errorDiv.style.display = 'block';
        return;
    }
    
    try {
        
        const response = await fetch('/api/auth/login', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ username, password })
        });
        
        const result = await response.json();
        
        if (result.success) {
            currentUser = result.user;
            updateUserUI(true);
            closeAuthModal();
            log('登录成功', 'success');
            // 登录后加载用户的自选基金
            await loadFavoriteFunds();
            // 登录后重新加载用户设置
            await loadConfig();
            // 登录后加载未读通知数量
            await loadUnreadNotificationCount();
            // 重新显示基金列表以更新自选状态
            if (allFundsData && allFundsData.length > 0) {
                displayFunds(allFundsData);
            }
        } else {
            errorDiv.textContent = result.message || '登录失败';
            errorDiv.style.display = 'block';
        }
    } catch (error) {
        errorDiv.textContent = '登录失败: ' + error.message;
        errorDiv.style.display = 'block';
    }
}

// 加载验证码
async function loadCaptcha() {
    try {
        const response = await fetch('/api/auth/captcha');
        const result = await response.json();
        if (result.success) {
            document.getElementById('captchaQuestion').textContent = result.question;
        } else {
            console.error('加载验证码失败:', result.message);
        }
    } catch (error) {
        console.error('加载验证码失败:', error);
    }
}

// 验证用户名格式
function validateUsername(username) {
    const trimmed = username.trim();
    const usernameError = document.getElementById('usernameError');
    const usernameSuccess = document.getElementById('usernameSuccess');
    const usernameHint = document.getElementById('usernameHint');
    
    // 隐藏所有提示
    usernameError.style.display = 'none';
    usernameSuccess.style.display = 'none';
    usernameHint.style.display = 'none';
    
    if (!trimmed) {
        usernameHint.style.display = 'block';
        return { valid: false, message: '' };
    }
    
    if (trimmed.length < 3) {
        usernameError.textContent = '用户名至少需要3个字符';
        usernameError.style.display = 'block';
        return { valid: false, message: '用户名至少需要3个字符' };
    }
    
    if (trimmed.length > 20) {
        usernameError.textContent = '用户名不能超过20个字符';
        usernameError.style.display = 'block';
        return { valid: false, message: '用户名不能超过20个字符' };
    }
    
    if (!/^[a-zA-Z0-9]+$/.test(trimmed)) {
        usernameError.textContent = '用户名只能包含字母和数字';
        usernameError.style.display = 'block';
        return { valid: false, message: '用户名只能包含字母和数字' };
    }
    
    // 格式验证通过，检查是否可用（异步）
    checkUsernameAvailability(trimmed);
    return { valid: true, message: '' };
}

// 检查用户名是否可用（异步）
let usernameCheckTimeout = null;
async function checkUsernameAvailability(username) {
    // 清除之前的定时器
    if (usernameCheckTimeout) {
        clearTimeout(usernameCheckTimeout);
    }
    
    // 延迟500ms后检查，避免频繁请求
    usernameCheckTimeout = setTimeout(async () => {
        try {
            const response = await fetch(`/api/auth/check-username?username=${encodeURIComponent(username)}`);
            const result = await response.json();
            
            const usernameError = document.getElementById('usernameError');
            const usernameSuccess = document.getElementById('usernameSuccess');
            
            if (result.available) {
                usernameError.style.display = 'none';
                usernameSuccess.style.display = 'block';
            } else {
                usernameError.textContent = result.message || '用户名已存在';
                usernameError.style.display = 'block';
                usernameSuccess.style.display = 'none';
            }
        } catch (error) {
            // 检查失败时不显示错误，只显示格式验证结果
            console.error('检查用户名可用性失败:', error);
        }
    }, 500);
}

// 验证邮箱格式
function validateEmail(email) {
    const trimmed = email.trim();
    const emailError = document.getElementById('emailError');
    const emailSuccess = document.getElementById('emailSuccess');
    const emailHint = document.getElementById('emailHint');
    
    // 隐藏所有提示
    emailError.style.display = 'none';
    emailSuccess.style.display = 'none';
    emailHint.style.display = 'none';
    
    // 邮箱是可选的，如果为空则不验证
    if (!trimmed) {
        emailHint.style.display = 'block';
        return { valid: true, message: '' };
    }
    
    // 邮箱格式验证正则表达式
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    
    if (!emailRegex.test(trimmed)) {
        emailError.textContent = '请输入有效的邮箱地址';
        emailError.style.display = 'block';
        return { valid: false, message: '请输入有效的邮箱地址' };
    }
    
    // 检查邮箱长度
    if (trimmed.length > 100) {
        emailError.textContent = '邮箱地址不能超过100个字符';
        emailError.style.display = 'block';
        return { valid: false, message: '邮箱地址不能超过100个字符' };
    }
    
    // 验证通过
    emailSuccess.style.display = 'block';
    return { valid: true, message: '' };
}

// 提交注册
async function submitRegister() {
    
    const username = document.getElementById('registerUsername').value.trim();
    const password = document.getElementById('registerPassword').value;
    const passwordConfirm = document.getElementById('registerPasswordConfirm').value;
    const email = document.getElementById('registerEmail').value.trim();
    const captchaAnswer = document.getElementById('registerCaptcha').value.trim();
    const errorDiv = document.getElementById('registerError');
    
    
    // 验证
    if (!username || !password || !passwordConfirm) {
        errorDiv.textContent = '请填写所有必填项';
        errorDiv.style.display = 'block';
        return;
    }
    
    if (password !== passwordConfirm) {
        errorDiv.textContent = '两次输入的密码不一致';
        errorDiv.style.display = 'block';
        return;
    }
    
    if (!captchaAnswer) {
        errorDiv.textContent = '请输入验证码';
        errorDiv.style.display = 'block';
        return;
    }
    
    try {
        
        const response = await fetch('/api/auth/register', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ 
                username, 
                password, 
                email: email || null,
                captcha_answer: captchaAnswer
            })
        });
        
        
        const result = await response.json();
        
        
        if (result.success) {
            log('注册成功，请登录', 'success');
            // 切换到登录标签
            switchAuthTab('login');
            document.getElementById('loginUsername').value = username;
        } else {
            errorDiv.textContent = result.message || '注册失败';
            errorDiv.style.display = 'block';
        }
    } catch (error) {
        errorDiv.textContent = '注册失败: ' + error.message;
        errorDiv.style.display = 'block';
    }
}

// 登出
async function logout() {
    // 登出由主界面统一管理：直接调主项目 /api/auth/logout（绕过 lof1 fetch 拦截器）
    try {
        await window._origFetch('/api/auth/logout', { method: 'POST' });
    } catch (_) {}
    window.top.location.href = '/';
}

// ==================== 管理员套利记录查看功能 ====================

// 打开管理员套利记录查看
async function openAdminArbitrageRecords() {
    
    if (!checkLogin()) {
        requireLogin();
        return;
    }
    
    // 检查是否为管理员
    if (!currentUser || currentUser.role !== 'admin') {
        alert('此功能需要管理员权限');
        return;
    }
    
    const modal = document.getElementById('adminArbitrageRecordsModal');
    
    if (!modal) {
        console.error('管理员套利记录模态框元素不存在');
        alert('管理员套利记录模态框元素不存在');
        return;
    }
    
    try {
        modal.classList.add('active');
        await loadAdminArbitrageStatistics();
        await loadAdminArbitrageRecords();
    } catch (error) {
        console.error('打开管理员套利记录失败:', error);
        alert('打开管理员套利记录失败: ' + error.message);
    }
}

// 关闭管理员套利记录查看
function closeAdminArbitrageRecords() {
    document.getElementById('adminArbitrageRecordsModal').classList.remove('active');
}

// 加载管理员套利记录统计
async function loadAdminArbitrageStatistics() {
    try {
        const response = await fetch('/api/admin/arbitrage/statistics');
        const result = await response.json();
        if (result.success) {
            displayAdminArbitrageStatistics(result.statistics);
        } else {
            document.getElementById('adminArbitrageStatsContent').innerHTML = 
                '<div style="color: red;">加载统计信息失败: ' + (result.message || '未知错误') + '</div>';
        }
    } catch (error) {
        document.getElementById('adminArbitrageStatsContent').innerHTML = 
            '<div style="color: red;">加载统计信息失败: ' + error.message + '</div>';
    }
}

// 显示管理员套利记录统计
function displayAdminArbitrageStatistics(stats) {
    const statsContent = document.getElementById('adminArbitrageStatsContent');
    
    let html = `
        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin-bottom: 15px;">
            <div>
                <strong>总记录数:</strong> ${stats.total_records}
            </div>
            <div>
                <strong>已完成:</strong> ${stats.total_completed}
            </div>
            <div>
                <strong>进行中:</strong> ${stats.total_in_progress}
            </div>
            <div>
                <strong>已取消:</strong> ${stats.total_cancelled}
            </div>
            <div>
                <strong>总盈亏:</strong> <span style="color: ${stats.total_profit >= 0 ? '#28a745' : '#dc3545'}">${stats.total_profit.toFixed(2)}</span>
            </div>
            <div>
                <strong>总金额:</strong> ${stats.total_amount.toFixed(2)}
            </div>
            <div>
                <strong>整体盈亏率:</strong> <span style="color: ${stats.overall_profit_rate >= 0 ? '#28a745' : '#dc3545'}">${stats.overall_profit_rate.toFixed(2)}%</span>
            </div>
        </div>
    `;
    
    if (Object.keys(stats.user_statistics).length > 0) {
        html += '<h4 style="margin-top: 15px; margin-bottom: 10px;">按用户统计:</h4>';
        html += '<div style="overflow-x: auto;"><table style="width: 100%; border-collapse: collapse; font-size: 12px;">';
        html += '<thead><tr style="background: #e9ecef;"><th style="padding: 8px; text-align: left;">用户名</th><th style="padding: 8px; text-align: right;">记录数</th><th style="padding: 8px; text-align: right;">已完成</th><th style="padding: 8px; text-align: right;">总盈亏</th><th style="padding: 8px; text-align: right;">盈亏率</th></tr></thead><tbody>';
        
        for (const [username, userStats] of Object.entries(stats.user_statistics)) {
            html += `
                <tr>
                    <td style="padding: 8px;">${username}</td>
                    <td style="padding: 8px; text-align: right;">${userStats.total_records}</td>
                    <td style="padding: 8px; text-align: right;">${userStats.completed}</td>
                    <td style="padding: 8px; text-align: right; color: ${userStats.total_profit >= 0 ? '#28a745' : '#dc3545'}">${userStats.total_profit.toFixed(2)}</td>
                    <td style="padding: 8px; text-align: right; color: ${userStats.profit_rate >= 0 ? '#28a745' : '#dc3545'}">${userStats.profit_rate.toFixed(2)}%</td>
                </tr>
            `;
        }
        
        html += '</tbody></table></div>';
    }
    
    statsContent.innerHTML = html;
}

// 加载管理员套利记录
async function loadAdminArbitrageRecords() {
    try {
        const statusFilter = document.getElementById('adminArbitrageStatusFilter').value;
        const fundCodeFilter = document.getElementById('adminArbitrageFundCodeFilter').value.trim();
        
        let url = '/api/admin/arbitrage/records?';
        if (statusFilter) url += `status=${statusFilter}&`;
        if (fundCodeFilter) url += `fund_code=${encodeURIComponent(fundCodeFilter)}&`;
        
        const response = await fetch(url);
        const result = await response.json();
        if (result.success) {
            displayAdminArbitrageRecords(result.records);
        } else {
            document.getElementById('adminArbitrageRecordsBody').innerHTML = 
                '<tr><td colspan="19" class="loading">加载失败: ' + (result.message || '未知错误') + '</td></tr>';
        }
    } catch (error) {
        document.getElementById('adminArbitrageRecordsBody').innerHTML = 
            '<tr><td colspan="19" class="loading">加载失败: ' + error.message + '</td></tr>';
    }
}

// 显示管理员套利记录
function displayAdminArbitrageRecords(records) {
    const tbody = document.getElementById('adminArbitrageRecordsBody');
    
    if (records.length === 0) {
        tbody.innerHTML = '<tr><td colspan="19" class="loading">暂无记录</td></tr>';
        return;
    }
    
    // 安全地格式化数字
    const formatNumber = (value, decimals = 2) => {
        if (value === null || value === undefined || isNaN(value)) {
            return '--';
        }
        return parseFloat(value).toFixed(decimals);
    };
    
    const formatPercent = (value) => {
        if (value === null || value === undefined || isNaN(value)) {
            return '--';
        }
        return (parseFloat(value) >= 0 ? '+' : '') + parseFloat(value).toFixed(2) + '%';
    };
    
    tbody.innerHTML = records.map(record => {
        const arbitrageTypeText = record.arbitrage_type === 'premium' ? '溢价套利' : '折价套利';
        const statusText = {
            'completed': '已完成',
            'in_progress': '进行中',
            'cancelled': '已取消',
            'pending': '待执行'
        }[record.status] || record.status;
        const statusClass = {
            'completed': 'status-badge',
            'in_progress': '',
            'cancelled': '',
            'pending': ''
        }[record.status] || '';
        
        const initial = record.initial_operation || {};
        const final = record.final_operation || null;
        
        // 初始操作信息
        const initialType = initial.type === 'subscribe' ? 
            (initial.fees?.operation_type === 'on_exchange' ? '场内申购' : '场外申购') : 
            (initial.type === 'buy' ? '买入' : '--');
        const initialDate = initial.date || '--';
        const initialAmount = initial.amount || 0;
        const initialFees = initial.fees || {};
        const initialFeeRate = initialFees.fee_rate || 0;
        const initialFeeAmount = initial.fee_amount || 0;
        let initialFeeDisplay = '--';
        let initialFeeTypeDisplay = '--';
        if (initialFeeRate > 0) {
            const feeType = initialFees.fee_type || '';
            if (feeType === 'buy_commission') {
                initialFeeTypeDisplay = '买入佣金';
            } else if (feeType === 'subscribe_fee') {
                initialFeeTypeDisplay = '申购费率';
            }
            initialFeeDisplay = `${(initialFeeRate * 100).toFixed(2)}%`;
        }
        
        // 最终操作信息
        const finalType = final ? (final.type === 'sell' ? '卖出' : '赎回') : '--';
        const finalDate = final && final.date ? final.date : '--';
        const finalAmount = final && final.amount !== undefined ? final.amount : null;
        const finalFees = final ? (final.fees || {}) : {};
        const finalFeeAmount = final ? (final.fee_amount || 0) : 0;
        let finalFeeDisplay = '--';
        let finalFeeTypeDisplay = '--';
        if (final) {
            if (record.arbitrage_type === 'premium') {
                const sellCommission = finalFees.sell_commission || 0;
                const stampTax = finalFees.stamp_tax || 0;
                const totalCostRate = finalFees.total_cost_rate || 0;
                if (totalCostRate > 0) {
                    finalFeeTypeDisplay = '卖出佣金+印花税';
                    finalFeeDisplay = `${(totalCostRate * 100).toFixed(2)}%`;
                }
            } else {
                const redeemFee = finalFees.redeem_fee || 0;
                if (redeemFee > 0) {
                    finalFeeTypeDisplay = '赎回费率';
                    finalFeeDisplay = `${(redeemFee * 100).toFixed(2)}%`;
                }
            }
        }
        
        // 盈亏信息
        const profit = record.profit !== null && record.profit !== undefined ? record.profit : null;
        const profitRate = record.profit_rate !== null && record.profit_rate !== undefined ? record.profit_rate : null;
        const netProfit = record.net_profit !== null && record.net_profit !== undefined ? record.net_profit : null;
        const netProfitRate = record.net_profit_rate !== null && record.net_profit_rate !== undefined ? record.net_profit_rate : null;
        
        const createdDate = record.created_at ? new Date(record.created_at).toLocaleString('zh-CN') : '--';
        
        return `
            <tr>
                <td style="padding: 8px;">${record.username || '--'}</td>
                <td style="padding: 8px;">${record.fund_code || '--'}</td>
                <td style="padding: 8px;">${record.fund_name || '--'}</td>
                <td style="padding: 8px;">${arbitrageTypeText}</td>
                <td style="padding: 8px;">${initialType}</td>
                <td style="padding: 8px;">${initialDate}</td>
                <td style="padding: 8px; text-align: right;">${formatNumber(initialAmount, 2)}</td>
                <td style="padding: 8px;" title="${initialFeeTypeDisplay}">${initialFeeDisplay}</td>
                <td style="padding: 8px; text-align: right;">${initialFeeAmount > 0 ? formatNumber(initialFeeAmount, 2) : '--'}</td>
                <td style="padding: 8px;">${finalType}</td>
                <td style="padding: 8px;">${finalDate}</td>
                <td style="padding: 8px; text-align: right;">${finalAmount !== null ? formatNumber(finalAmount, 2) : '--'}</td>
                <td style="padding: 8px;" title="${finalFeeTypeDisplay}">${finalFeeDisplay}</td>
                <td style="padding: 8px; text-align: right;">${finalFeeAmount > 0 ? formatNumber(finalFeeAmount, 2) : '--'}</td>
                <td style="padding: 8px; text-align: right; color: ${profit !== null ? (profit >= 0 ? '#28a745' : '#dc3545') : '#666'}">${profit !== null ? formatNumber(profit, 2) : '--'}</td>
                <td style="padding: 8px; text-align: right; color: ${profitRate !== null ? (profitRate >= 0 ? '#28a745' : '#dc3545') : '#666'}">${profitRate !== null ? formatPercent(profitRate) : '--'}</td>
                <td style="padding: 8px; text-align: right; color: ${netProfit !== null ? (netProfit >= 0 ? '#28a745' : '#dc3545') : '#666'}">${netProfit !== null ? formatNumber(netProfit, 2) : '--'}</td>
                <td style="padding: 8px; text-align: right; color: ${netProfitRate !== null ? (netProfitRate >= 0 ? '#28a745' : '#dc3545') : '#666'}">${netProfitRate !== null ? formatPercent(netProfitRate) : '--'}</td>
                <td style="padding: 8px;"><span class="${statusClass}">${statusText}</span></td>
                <td style="padding: 8px;">${createdDate}</td>
            </tr>
        `;
    }).join('');
}

// 应用筛选
function applyAdminArbitrageFilter() {
    loadAdminArbitrageRecords();
}

// ==================== 通知相关功能 ====================

// 打开通知列表
async function openNotifications() {
    if (!checkLogin()) {
        requireLogin();
        return;
    }
    
    document.getElementById('notificationModal').classList.add('active');
    await loadNotifications();
}

// 关闭通知列表
function closeNotifications() {
    document.getElementById('notificationModal').classList.remove('active');
}

// 加载通知列表
async function loadNotifications() {
    try {
        const response = await fetch('/api/notifications');
        const result = await response.json();
        if (result.success) {
            displayNotifications(result.notifications);
        } else {
            document.getElementById('notificationsList').innerHTML = 
                '<div style="color: red;">加载失败: ' + (result.message || '未知错误') + '</div>';
        }
    } catch (error) {
        document.getElementById('notificationsList').innerHTML = 
            '<div style="color: red;">加载失败: ' + error.message + '</div>';
    }
}

// 显示通知列表
function displayNotifications(notifications) {
    const container = document.getElementById('notificationsList');
    
    if (notifications.length === 0) {
        container.innerHTML = '<div class="loading" style="text-align: center; padding: 40px; color: #999;">暂无通知</div>';
        return;
    }
    
    container.innerHTML = notifications.map(notification => {
        const isRead = notification.read;
        const createdDate = notification.created_at ? 
            new Date(notification.created_at).toLocaleString('zh-CN') : '--';
        
        return `
            <div class="notification-item ${isRead ? 'read' : 'unread'}" 
                 onclick="handleNotificationClick('${notification.id}', ${!isRead})">
                <div class="notification-content">
                    <div class="notification-header">
                        <div class="notification-title-row">
                            <strong class="notification-title">${notification.title}</strong>
                            ${!isRead ? '<span class="notification-badge">未读</span>' : ''}
                        </div>
                        <button class="btn btn-small btn-secondary notification-delete-btn" 
                                onclick="event.stopPropagation(); deleteNotification('${notification.id}')">删除</button>
                    </div>
                    <div class="notification-body">
                        <div class="notification-text">${notification.content}</div>
                        <div class="notification-time">${createdDate}</div>
                    </div>
                </div>
            </div>
        `;
    }).join('');
}

// 处理通知点击
async function handleNotificationClick(notificationId, isUnread) {
    if (isUnread) {
        await markNotificationRead(notificationId);
    }
    // 可以在这里添加跳转到相关页面的逻辑
}

// 标记通知为已读
async function markNotificationRead(notificationId) {
    try {
        const response = await fetch(`/api/notifications/${notificationId}/read`, {
            method: 'POST'
        });
        const result = await response.json();
        if (result.success) {
            await loadNotifications();
            await loadUnreadNotificationCount();
        }
    } catch (error) {
        console.error('标记通知已读失败:', error);
    }
}

// 删除通知
async function deleteNotification(notificationId) {
    if (!confirm('确定要删除这条通知吗？')) {
        return;
    }
    
    try {
        const response = await fetch(`/api/notifications/${notificationId}`, {
            method: 'DELETE'
        });
        const result = await response.json();
        if (result.success) {
            await loadNotifications();
            await loadUnreadNotificationCount();
        } else {
            alert('删除失败: ' + (result.message || '未知错误'));
        }
    } catch (error) {
        alert('删除失败: ' + error.message);
    }
}

// 标记所有通知为已读
async function markAllNotificationsRead() {
    try {
        const response = await fetch('/api/notifications/read-all', {
            method: 'POST'
        });
        const result = await response.json();
        if (result.success) {
            await loadNotifications();
            await loadUnreadNotificationCount();
        } else {
            alert('操作失败: ' + (result.message || '未知错误'));
        }
    } catch (error) {
        alert('操作失败: ' + error.message);
    }
}

// 删除所有已读通知
async function deleteAllReadNotifications() {
    if (!confirm('确定要删除所有已读通知吗？')) {
        return;
    }
    
    try {
        const response = await fetch('/api/notifications/delete-read', {
            method: 'POST'
        });
        const result = await response.json();
        if (result.success) {
            await loadNotifications();
            await loadUnreadNotificationCount();
        } else {
            alert('操作失败: ' + (result.message || '未知错误'));
        }
    } catch (error) {
        alert('操作失败: ' + error.message);
    }
}

// 加载未读通知数量
async function loadUnreadNotificationCount() {
    if (!checkLogin()) {
        const notificationContainer = document.getElementById('notificationContainer');
        if (notificationContainer) {
            notificationContainer.style.display = 'none';
        }
        return;
    }
    
    try {
        const response = await fetch('/api/notifications/unread-count');
        const result = await response.json();
        if (result.success) {
            updateNotificationBadge(result.count);
        }
    } catch (error) {
        console.error('加载未读通知数量失败:', error);
    }
}

// 更新通知徽章
function updateNotificationBadge(count) {
    const notificationContainer = document.getElementById('notificationContainer');
    const notificationBadge = document.getElementById('notificationBadge');
    
    if (notificationContainer) {
        notificationContainer.style.display = 'inline-block';
    }
    
    if (notificationBadge) {
        if (count > 0) {
            notificationBadge.textContent = count > 99 ? '99+' : count;
            notificationBadge.style.display = 'block';
        } else {
            notificationBadge.style.display = 'none';
        }
    }
}

// 刷新单个基金的申购状态
async function refreshFundPurchaseLimit(fundCode) {
    const statusElement = document.getElementById('detailPurchaseStatus');
    if (!statusElement) return;
    
    const originalText = statusElement.textContent;
    statusElement.textContent = '刷新中...';
    statusElement.style.color = '#666';
    
    try {
        const response = await fetch(`/api/funds/${fundCode}/refresh-purchase-limit`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            }
        });
        
        const result = await response.json();
        
        if (result.success && result.purchase_limit) {
            const purchaseStatus = result.purchase_limit.purchase_status || result.purchase_limit.limit_desc || '开放申购';
            statusElement.textContent = purchaseStatus;
            
            // 根据状态设置颜色
            if (purchaseStatus === '暂停申购') {
                statusElement.style.color = '#f44336';
            } else if (purchaseStatus.includes('限购')) {
                statusElement.style.color = '#ff9800';
            } else {
                statusElement.style.color = '#4CAF50';
            }
            
            log(`基金 ${fundCode} 申购状态已刷新: ${purchaseStatus}`, 'success');
            
            // 同时更新列表中的申购状态（如果该基金在列表中）
            if (allFundsData && allFundsData.length > 0) {
                const fundIndex = allFundsData.findIndex(f => f.fund_code === fundCode);
                if (fundIndex >= 0) {
                    allFundsData[fundIndex].purchase_limit = result.purchase_limit;
                    // 重新显示基金列表
                    displayFunds(allFundsData);
                }
            }
        } else {
            statusElement.textContent = originalText;
            statusElement.style.color = '#666';
            log('刷新申购状态失败: ' + (result.message || '未知错误'), 'error');
        }
    } catch (error) {
        statusElement.textContent = originalText;
        statusElement.style.color = '#666';
        log('刷新申购状态失败: ' + error.message, 'error');
    }
}

// 手机端菜单控制
function openMobileMenu() {
    const menu = document.getElementById('mobileMenu');
    if (menu) {
        menu.classList.add('active');
        menu.style.display = 'block';
        menu.setAttribute('aria-hidden', 'false');
        document.body.style.overflow = 'hidden';
        // 同步显示/隐藏管理员按钮
        const dataSourceBtn = document.getElementById('dataSourceConfigBtn');
        const adminRecordsBtn = document.getElementById('adminArbitrageRecordsBtn');
        const loginBtn = document.getElementById('loginBtn');
        const logoutBtn = document.getElementById('logoutBtn');
        const userInfo = document.getElementById('userInfo');
        
        const mobileDataSourceBtn = document.getElementById('mobileDataSourceConfigBtn');
        const mobileAdminRecordsBtn = document.getElementById('mobileAdminArbitrageRecordsBtn');
        const mobileLoginBtn = document.getElementById('mobileLoginBtn');
        const mobileLogoutBtn = document.getElementById('mobileLogoutBtn');
        
        if (mobileDataSourceBtn) {
            mobileDataSourceBtn.style.display = (dataSourceBtn && dataSourceBtn.style.display !== 'none') ? 'block' : 'none';
        }
        if (mobileAdminRecordsBtn) {
            mobileAdminRecordsBtn.style.display = (adminRecordsBtn && adminRecordsBtn.style.display !== 'none') ? 'block' : 'none';
        }
        if (mobileLoginBtn) {
            mobileLoginBtn.style.display = (loginBtn && loginBtn.style.display !== 'none') ? 'block' : 'none';
        }
        if (mobileLogoutBtn) {
            mobileLogoutBtn.style.display = (userInfo && userInfo.style.display !== 'none') ? 'block' : 'none';
        }
    }
}

function closeMobileMenu() {
    const menu = document.getElementById('mobileMenu');
    if (menu) {
        menu.classList.remove('active');
        menu.style.display = '';
        menu.setAttribute('aria-hidden', 'true');
        document.body.style.overflow = '';
    }
}

// 开始定期检查通知
function startNotificationCheck() {
    // 每30秒检查一次未读通知数量
    if (notificationCheckInterval) {
        clearInterval(notificationCheckInterval);
    }
    notificationCheckInterval = setInterval(() => {
        if (checkLogin()) {
            loadUnreadNotificationCount();
        }
    }, 30000); // 30秒
}

// 点击模态框外部关闭
window.addEventListener('click', function(event) {
    const authModal = document.getElementById('authModal');
    if (event.target === authModal) {
        closeAuthModal();
    }
});
