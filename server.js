// server.js

import dotenv from "dotenv";
dotenv.config();

import express from "express";
import { SYMBOL_REGISTRY } from "./config/symbols.js";
import mongoose from "mongoose";
import cors from "cors";
import { createServer } from "http";
import { Server } from "socket.io";
import authRoutes from "./routes/auth.js";
import adminRoutes from "./routes/admin.js";
import accountTypesRoutes from "./routes/accountTypes.js";
import tradingAccountsRoutes from "./routes/tradingAccounts.js";
import walletRoutes from "./routes/wallet.js";
import paymentMethodsRoutes from "./routes/paymentMethods.js";
import tradeRoutes from "./routes/trade.js";
import walletTransferRoutes from "./routes/walletTransfer.js";
import adminTradeRoutes from "./routes/adminTrade.js";
import copyTradingRoutes from "./routes/copyTrading.js";
import ibRoutes from "./routes/ibNew.js";
import propTradingRoutes from "./routes/propTrading.js";
import chargesRoutes from "./routes/charges.js";
import pricesRoutes from "./routes/prices.js";
import earningsRoutes from "./routes/earnings.js";
import supportRoutes from "./routes/support.js";
import kycRoutes from "./routes/kyc.js";
import themeRoutes from "./routes/theme.js";
import adminManagementRoutes from "./routes/adminManagement.js";
import uploadRoutes from "./routes/upload.js";
import emailRoutes from "./routes/email.js";
import oxapayRoutes from "./routes/oxapay.js";
import emailService from "./services/emailService.js";
import bannerRoutes from "./routes/banner.js";
import carouselRoutes from "./routes/carousel.js";
import warmupService from "./services/warmupService.js";
import path from "path";
import { fileURLToPath } from "url";
import alltickApiService from "./services/alltickApiService.js";
import binanceRoutes from "./routes/binance.js";
import marketRoutes from "./routes/market.js";
import storageService from "./services/storageService.js"; // //sanket - Import storage service
import tradeEngine from "./services/tradeEngine.js";
import propTradingEngine from "./services/propTradingEngine.js";
import competitionGuardian from "./services/competitionGuardian.js";
import competitionRoutes from "./routes/competitionRoutes.js";
import adminUserRoutes from "./routes/adminUserRoutes.js";
import internalTransferRoutes from "./routes/internalTransfer.js";
import redisClient from "./services/redisClient.js";
import competitionLeaderboard from "./routes/competitionLeaderboard.js";
import websiteRoutes from "./routes/website.js";
import competitionEmailRoutes from "./routes/emailRoutes.js";
import { initCompetitionCron } from "./utils/competitionStatusCron.js";
import announcementRoute from "./routes/announcements_Router.js";
import notificationRoutes from "./routes/notificationRoutes.js";
import chartRoutes from "./routes/chart.js";
import chartHistoryRoutes from "./routes/chartHistory.js";
import withdrawLimitRoutes from "./routes/withdrawableLimitRoute.js";
import deleteTradingAccountRoute from "./routes/deleteTradingAccountRoute.js";
import mobileRoutes from "./routes/mobile/index.js";
import modularIbRoutes from "./modules/ib/routes/ibRoutes.js";
import { startIBWorker } from "./modules/ib/services/ibWorker.js";
import ibRuleEngine from "./modules/ib/services/ibRuleEngine.js";
import marketInsightRoutes from "./routes/marketInsight.js";
import currencyStrengthRoutes from "./routes/currencyStrength.js";
import { initMarketInsightCron } from "./utils/marketInsightCron.js";
import watchlistSocketService from "./services/watchlistSocketService.js";
import tvDatafeedRoutes from "./routes/tvDatafeed.js";
import mobileTvDatafeedRoutes from "./routes/mobileTvDatafeed.js";
import candleEngine from "./charting/CandleEngine.js";
import { setGlobalIo } from "./services/notificationService.js";
import syncWorker from "./charting/SyncWorker.js";
import startKYCPendingReminderCron from "./cron/adminPendingReminderCron.js";
import startComplianceMonitorCron from "./cron/compliance/complianceMonitorCron.js";

import startRiskAlertCron from "./cron/riskAlertCron.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

dotenv.config();

const app = express();
const httpServer = createServer(app);

//Sanket v2.0 - initialize the email provider and retry scheduler as soon as the backend boots so failed sends can self-recover
emailService.initialize().catch((error) => {
  console.error("[Email] Initialization failed:", error.message);
});
// initLiveSocket(httpServer);

// Socket.IO for real-time updates
const io = new Server(httpServer, {
  cors: {
    origin: "*",
    methods: ["GET", "POST", "PUT", "DELETE"],
  },
  transports: ["websocket"], // //sanket - Force WebSockets for lower latency and better stability
  pingTimeout: 30000,
  pingInterval: 10000,
});

// ✅ Store io in app context so routes can access it
app.set("io", io);

// ✅ Initialize Watchlist Socket Service
watchlistSocketService.initialize(io);

// ✅ Link IO to Notification Service for background alerts
setGlobalIo(io);

// Store connected clients
const connectedClients = new Map();
const priceSubscribers = new Set();
const socketPrioritySymbols = new Map();
let isShuttingDown = false;
let broadcastInterval = null;
let syncInterval = null;
let slTpCheckInterval = null;

function refreshPrioritySymbols() {
  const rooms = io.sockets.adapter.rooms;
  const activeChartSymbols = [];

  for (const [roomName, sockets] of rooms.entries()) {
    if (roomName.startsWith("candles:")) {
      const symbol = roomName.split(":")[1];
      if (symbol && sockets.size > 0) {
        activeChartSymbols.push(symbol);
      }
    }
  }

  console.log(
    `[Server] Refreshing priority symbols for ${activeChartSymbols.length} active charts: ${activeChartSymbols.join(", ")}`,
  );

  if (activeChartSymbols.length > 0) {
    alltickApiService.setPrioritySymbols(activeChartSymbols);
  }
}

const ENABLE_LIVE_PERSIST =
  (process.env.ENABLE_LIVE_PERSIST || "true").toLowerCase() !== "false";
const ENABLE_PERIODIC_HISTORY_SYNC =
  (process.env.ENABLE_PERIODIC_HISTORY_SYNC || "true").toLowerCase() !==
  "false";
// Initialize market data connections
console.log("[Server] Initializing AllTick market data service...");
alltickApiService.connect();

// ✅ Elite Performance: Start Redis cache warmup for top symbols
// This ensures popular charts load instantly from the moment the server is live
warmupService
  .run()
  .catch((err) => console.error("[Server] Warmup failed:", err.message));

// ELITE: Charge caching to prevent DB thrashing during high-frequency candle updates
const chargeCache = new Map();
const CHARGE_CACHE_TTL = 30000; // 30 seconds

// ✅ Backend Candle Authority: Bridge CandleEngine updates to Socket.io
candleEngine.on("candleUpdate", async (data) => {
  if (isShuttingDown) return;

  // ELITE: Institutional Visual Synchronization
  let shiftedCandle = { ...data.candle };

  try {
    const meta = SYMBOL_REGISTRY[data.symbol];
    const pipValue = meta?.pipValue || 0.0001;

    // We fetch the default charge for this symbol
    const Charges = mongoose.model("Charges");
    const charge = await Charges.findOne({
      symbol: data.symbol,
      isDefault: true,
      isActive: true,
    });
    const spreadValue = charge ? charge.spreadValue : 0;
    const shift = (spreadValue * pipValue) / 2;

    shiftedCandle.open -= shift;
    shiftedCandle.high -= shift;
    shiftedCandle.low -= shift;
    shiftedCandle.close -= shift;
  } catch (err) {
    // Fallback to raw if lookup fails
  }

  io.to(`candles:${data.symbol}`).emit("candleUpdate", {
    symbol: data.symbol,
    timeframe: data.timeframe,
    candle: shiftedCandle,
  });
});
console.log(
  `[Server] Feature flags -> ENABLE_LIVE_PERSIST=${ENABLE_LIVE_PERSIST}, ENABLE_PERIODIC_HISTORY_SYNC=${ENABLE_PERIODIC_HISTORY_SYNC}`,
);

// Track last emit times to prevent flooding
const lastEmitTimes = new Map();
const TICK_THROTTLE_MS = 500; // Sidebar symbols updated twice per second

// Stream incremental price updates from Redis (Decoupled Pub/Sub)
const redisSubscriber = redisClient.duplicate();

redisSubscriber.subscribe("price_updates", (err, count) => {
  if (err) console.error("[Redis Pub/Sub] Subscription failed:", err.message);
  else console.log(`[Redis Pub/Sub] Subscribed to ${count} channels.`);
});

redisSubscriber.on("message", (channel, message) => {
  if (channel === "price_updates") {
    try {
      const priceData = JSON.parse(message);
      const symbol = priceData.symbol;

      if (isShuttingDown) return;
      if (!symbol || !priceData) return;

      const now = Date.now();

      const activeChartSymbols = new Set(
        [...socketPrioritySymbols.values()].flatMap((syms) => syms || []),
      );

      const isBeingWatched = activeChartSymbols.has(symbol);

      if (isBeingWatched && priceSubscribers.size > 0) {
        const payload = {
          symbol,
          bid: priceData.bid,
          ask: priceData.ask,
          time: priceData.time || now,
          provider: priceData.provider || "alltick",
        };

        io.to("prices").emit("tickUpdate", payload);
      }

      if (ENABLE_LIVE_PERSIST && !priceData.mappedFrom) {
        candleEngine._handleTick(symbol, priceData).catch(() => {});
      }
    } catch (err) {
      console.error("[Redis Pub/Sub] Message parse error:", err.message);
    }
  }
});

// Broadcast prices to connected clients every 1000ms
broadcastInterval = setInterval(async () => {
  if (priceSubscribers.size === 0) return;

  const now = Date.now();
  const [allPrices, pricesByCategory] = await Promise.all([
    alltickApiService.getAllPrices(),
    alltickApiService.getPricesByCategory(),
  ]);

  // Broadcast prices by category and all prices
  io.to("prices").emit("priceStream", {
    prices: allPrices,
    categories: pricesByCategory,
    timestamp: now,
    provider: "alltick",
  });
}, 1000);

// --- AUTO SL/TP, PENDING & STOP-OUT CHECKER ---
// Runs every 1 second to verify SL/TP, Pending Orders, and Margin Requirements (Stop Out)
slTpCheckInterval = setInterval(async () => {
  if (isShuttingDown) return;
  try {
    const allPrices = await alltickApiService.getAllPrices();
    if (!allPrices || Object.keys(allPrices).length === 0) return;

    // 1. Check SL/TP for all Regular and Challenge Trades
    const closedChallenge =
      await propTradingEngine.checkSlTpForAllTrades(allPrices);
    const closedRegular = await tradeEngine.checkSlTpForAllTrades(allPrices);

    const allClosed = [...closedChallenge, ...closedRegular];

    if (allClosed.length > 0) {
      console.log(
        `[TradeEngine] Natively auto-closed ${allClosed.length} trades via SL/TP hit.`,
      );
      // Emit closure to specific users
      allClosed.forEach((ct) => {
        if (ct.trade && ct.trade.tradingAccountId) {
          app
            .get("io")
            .to(`account:${ct.trade.tradingAccountId}`)
            .emit("tradeClosed", ct.trade);
        }
      });
    }

    // 2. Check Pending Orders
    const executedPending = await tradeEngine.checkPendingOrders(allPrices);
    if (executedPending && executedPending.length > 0) {
      console.log(
        `[TradeEngine] Auto-executed ${executedPending.length} pending orders.`,
      );
      executedPending.forEach((et) => {
        if (et.trade && et.trade.tradingAccountId) {
          app
            .get("io")
            .to(`account:${et.trade.tradingAccountId}`)
            .emit("tradeUpdated", et.trade);
        }
      });
    }

    // 3. ELITE FIX: Background Stop-Out Checker
    // This protects the broker from negative balance by liquidating accounts that breach margin levels
    const stopOutResults =
      await tradeEngine.checkStopOutForAllAccounts(allPrices);
    if (stopOutResults && stopOutResults.length > 0) {
      stopOutResults.forEach((res) => {
        io.to(`account:${res.tradingAccountId}`).emit("stopOutOccurred", res);
      });
    }

    // 3.5 🛡️ GUARDIAN ENGINE: Check Competition Violations (Drawdowns)
    // Only run if DB is connected to prevent "Closed connection pool" errors
    if (mongoose.connection.readyState === 1) {
      await competitionGuardian.checkRules(io, allPrices);
    }

    // 4. ELITE FIX: Real-time Financials Stream (Equity/Margin)
    // This solves the 'Patchwork' issue where mobile users had to wait 5s for updates.
    // We only update accounts that are currently connected and have open trades.
    const activeAccountIds = new Set(connectedClients.values());
    if (activeAccountIds.size > 0) {
      // Batch calculate summaries to prevent event-loop lag
      for (const accountId of activeAccountIds) {
        const openTrades = await mongoose
          .model("Trade")
          .find({ tradingAccountId: accountId, status: "OPEN" });
        if (openTrades.length === 0) continue;

        const summary = await tradeEngine.getAccountSummary(
          accountId,
          openTrades,
          allPrices,
        );

        // Emit only to sockets belonging to this account
        io.to(`account:${accountId}`).emit("financialsUpdate", {
          id: accountId,
          balance: summary.balance,
          equity: summary.equity,
          usedMargin: summary.usedMargin,
          freeMargin: summary.freeMargin,
          marginLevel: summary.marginLevel,
          floatingPnl: summary.floatingPnl,
        });
      }
    }
  } catch (err) {
    // console.error('[TradeEngine] Background Check Error:', err.message);
  }
}, 1000);

const gracefulShutdown = async (signal) => {
  if (isShuttingDown) return;
  isShuttingDown = true;

  console.log(`[Server] Received ${signal}. Starting graceful shutdown...`);

  try {
    if (broadcastInterval) clearInterval(broadcastInterval);
    if (syncInterval) clearInterval(syncInterval);
    if (slTpCheckInterval) clearInterval(slTpCheckInterval);

    if (ENABLE_LIVE_PERSIST) {
      const finalStats = await storageService.shutdown();
      console.log("[Server] Storage flush complete:", {
        writes: finalStats.livePersistedWrites,
        errors: finalStats.livePersistErrors,
        pending: finalStats.pendingLiveBarOps,
      });
    }

    await new Promise((resolve) => io.close(resolve));
    await new Promise((resolve) => httpServer.close(resolve));

    await mongoose.connection.close(false);
    console.log("[Server] Graceful shutdown completed.");
    process.exit(0);
  } catch (error) {
    console.error("[Server] Graceful shutdown failed:", error.message);
    process.exit(1);
  }
};

process.on("SIGINT", () => {
  gracefulShutdown("SIGINT");
});

process.on("SIGTERM", () => {
  gracefulShutdown("SIGTERM");
});
/* 
io.on('connection', (socket) => {
  console.log('Client connected:', socket.id)

  // Subscribe to real-time price stream
  socket.on('subscribePrices', async () => {
    socket.join('prices')
    priceSubscribers.add(socket.id)
    // Send current prices immediately
    
    // Await Redis prices
    const initialPrices = await alltickApiService.getAllPrices();
    
    socket.emit('priceStream', {
      prices: initialPrices,
      updated: {},
      timestamp: Date.now()
    })
    console.log(`Socket ${socket.id} subscribed to price stream`)
  })

  socket.on('setPrioritySymbols', (data) => {
    const symbols = Array.isArray(data?.symbols) ? data.symbols : []
    socketPrioritySymbols.set(socket.id, symbols)
    const appliedSymbols = alltickApiService.setPrioritySymbols([...new Set(
      [...socketPrioritySymbols.values()].flatMap(items => items || [])
    )])
    if (appliedSymbols.length > 0) {
      console.log(`Socket ${socket.id} set priority symbols: ${appliedSymbols.join(', ')}`)
    }
  })

  // Unsubscribe from price stream
  socket.on('unsubscribePrices', () => {
    socket.leave('prices')
    priceSubscribers.delete(socket.id)
    socketPrioritySymbols.delete(socket.id)
    refreshPrioritySymbols()
  })

  // Subscribe to account updates
  socket.on('subscribe', (data) => {
    const { tradingAccountId } = data
    if (tradingAccountId) {
      socket.join(`account:${tradingAccountId}`)
      connectedClients.set(socket.id, tradingAccountId)
      console.log(`Socket ${socket.id} subscribed to account ${tradingAccountId}`)
    }
  })

  // ✅ NEW: Subscribe to user notifications
  socket.on('join', (userId) => {
    if (userId) {
      socket.join(userId.toString());
      socket.join(`user_${userId}`);
      console.log(`Socket ${socket.id} joined notification room for user ${userId}`);
    }
  });

  // ✅ Backend Candle Authority: Join resolution-agnostic symbol room
  socket.on('subscribeBars', (data) => {
    const { symbol } = data;
    if (symbol) {
      socket.join(`candles:${symbol}`);
      console.log(`Socket ${socket.id} joined candle room for ${symbol}`);
    }
  });

  socket.on('unsubscribeBars', (data) => {
    const { symbol } = data;
    if (symbol) {
      socket.leave(`candles:${symbol}`);
    }
  });

  // Unsubscribe from account updates
  socket.on('unsubscribe', (data) => {
    const { tradingAccountId } = data
    if (tradingAccountId) {
      socket.leave(`account:${tradingAccountId}`)
      connectedClients.delete(socket.id)
    }
  })

  // Handle price updates from client (for PnL calculation)
  socket.on('priceUpdate', async (data) => {
    const { tradingAccountId, prices } = data
    if (tradingAccountId && prices) {
      // Broadcast updated account summary to all subscribers
      io.to(`account:${tradingAccountId}`).emit('accountUpdate', {
        tradingAccountId,
        prices,
        timestamp: Date.now()
      })
    }
  })

  socket.on('disconnect', () => {
    connectedClients.delete(socket.id)
    priceSubscribers.delete(socket.id)
    socketPrioritySymbols.delete(socket.id)
    refreshPrioritySymbols()
    console.log('Client disconnected:', socket.id)
  })
})
 */

io.on("connection", (socket) => {
  console.log("🟢 Client Connected:", socket.id);

  // =============================
  // ✅ JOIN USER ROOM (FOR NOTIFICATIONS)
  // =============================
  socket.on("joinUserRoom", (userId) => {
    if (!userId) {
      console.log("❌ No userId provided for room join");
      return;
    }

    socket.join(userId.toString());
    console.log(`👤 User ${userId} joined room`);
  });

  // =============================
  // PRICE SUBSCRIBE
  // =============================
  socket.on("subscribePrices", async () => {
    socket.join("prices");
    priceSubscribers.add(socket.id);

    console.log(`📊 ${socket.id} subscribed to prices`);

    const initialPrices = await alltickApiService.getAllPrices();

    socket.emit("priceStream", {
      prices: initialPrices,
      updated: {},
      timestamp: Date.now(),
    });
  });

  socket.on("setPrioritySymbols", (data) => {
    const symbols = Array.isArray(data?.symbols) ? data.symbols : [];
    socketPrioritySymbols.set(socket.id, symbols);

    const appliedSymbols = alltickApiService.setPrioritySymbols([
      ...new Set(
        [...socketPrioritySymbols.values()].flatMap((items) => items || []),
      ),
    ]);

    if (appliedSymbols.length > 0) {
      console.log(`⚡ Priority symbols: ${appliedSymbols.join(", ")}`);
    }
  });

  socket.on("unsubscribePrices", () => {
    socket.leave("prices");
    priceSubscribers.delete(socket.id);
    socketPrioritySymbols.delete(socket.id);
    refreshPrioritySymbols();
  });

  // =============================
  // ACCOUNT SUBSCRIBE
  // =============================
  socket.on("subscribe", (data) => {
    const { tradingAccountId } = data;
    if (tradingAccountId) {
      socket.join(`account:${tradingAccountId}`);
      connectedClients.set(socket.id, tradingAccountId);

      console.log(`📊 ${socket.id} subscribed to account ${tradingAccountId}`);
    }
  });

  socket.on("unsubscribe", (data) => {
    const { tradingAccountId } = data;
    if (tradingAccountId) {
      socket.leave(`account:${tradingAccountId}`);
      connectedClients.delete(socket.id);
    }
  });

  // =============================
  // CANDLE SUBSCRIBE
  // =============================
  socket.on("subscribeBars", (data) => {
    const { symbol } = data;
    if (symbol) {
      socket.join(`candles:${symbol}`);
      console.log(`📈 ${socket.id} joined candle room ${symbol}`);
      refreshPrioritySymbols();
    }
  });

  socket.on("unsubscribeBars", (data) => {
    const { symbol } = data;
    if (symbol) {
      socket.leave(`candles:${symbol}`);
      refreshPrioritySymbols();
    }
  });

  // =============================
  // PRICE UPDATE (PnL)
  // =============================
  socket.on("priceUpdate", async (data) => {
    const { tradingAccountId, prices } = data;

    if (tradingAccountId && prices) {
      io.to(`account:${tradingAccountId}`).emit("accountUpdate", {
        tradingAccountId,
        prices,
        timestamp: Date.now(),
      });
    }
  });

  // =============================
  // WATCHLIST SUBSCRIBE (Mobile)
  // =============================
  socket.on("SUBSCRIBE_WATCHLIST", (data) => {
    const { symbols } = data;
    if (Array.isArray(symbols)) {
      watchlistSocketService.subscribe(socket, symbols);
    }
  });

  socket.on("UNSUBSCRIBE_WATCHLIST", (data) => {
    const { symbols } = data;
    if (Array.isArray(symbols)) {
      watchlistSocketService.unsubscribe(socket, symbols);
    }
  });

  // =============================
  // DISCONNECT
  // =============================
  socket.on("disconnect", () => {
    connectedClients.delete(socket.id);
    priceSubscribers.delete(socket.id);
    socketPrioritySymbols.delete(socket.id);
    refreshPrioritySymbols();
    watchlistSocketService.handleDisconnect(socket.id);

    console.log("🔴 Client Disconnected:", socket.id);
  });
});

// Make io accessible to routes
app.set("io", io);

// Middleware - CORS configuration for production
const allowedOrigins = [
  "https://trade.hcfinvest.com",
  "https://testing.hcfinvest.com",
  "https://hcfinvest.com",
  "https://www.hcfinvest.com",
  "https://admin.hcfinvest.com",
  "https://heddgecapitals.com",
  "https://www.heddgecapitals.com",
  "https://www.hcfinvest.com",
  "https://hcfinvest.com",
  "http://localhost:5173",
  "http://localhost:3000",
];

const corsOptions = {
  origin: (origin, callback) => {
    // Allow requests with no origin (like mobile apps or curl)
    if (!origin) return callback(null, true);
    if (
      allowedOrigins.indexOf(origin) !== -1 ||
      origin.startsWith("http://localhost")
    ) {
      callback(null, true);
    } else {
      console.warn(`[CORS] Blocked request from origin: ${origin}`);
      callback(new Error("CORS policy: Origin not allowed"));
    }
  },
  credentials: true,
  methods: ["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"],
  allowedHeaders: [
    "Content-Type",
    "Authorization",
    "X-Requested-With",
    "auth-token",
  ],
};

app.use(cors(corsOptions));
app.use(express.json({ limit: "50mb" }));
app.use(express.urlencoded({ limit: "50mb", extended: true }));

// Connect to MongoDB
mongoose
  .connect(process.env.MONGODB_URI)
  .then(() => console.log("Connected to MongoDB"))
  .catch((err) => console.error("MongoDB connection error:", err));

// Routes
app.use("/api/auth", authRoutes);
app.use("/api/admin", adminRoutes);
app.use("/api/account-types", accountTypesRoutes);
app.use("/api/trading-accounts", tradingAccountsRoutes);
app.use("/api/wallet", walletRoutes);
app.use("/api/payment-methods", paymentMethodsRoutes);
app.use("/api/trade", tradeRoutes);
app.use("/api/wallet-transfer", walletTransferRoutes);
app.use("/api/admin/trade", adminTradeRoutes);
app.use("/api/copy", copyTradingRoutes);
app.use("/api/ib", ibRoutes);
app.use("/api/website", websiteRoutes);
app.use("/api/modular-ib", modularIbRoutes);
app.use("/api/prop", propTradingRoutes);
app.use("/api/charges", chargesRoutes);
app.use("/api/prices", pricesRoutes);
app.use("/api/earnings", earningsRoutes);
app.use("/api/support", supportRoutes);
app.use("/api/kyc", kycRoutes);
app.use("/api/theme", themeRoutes);
app.use("/api/admin-mgmt", adminManagementRoutes);
app.use("/api/upload", uploadRoutes);
app.use("/api/email", emailRoutes);
app.use("/api/oxapay", oxapayRoutes);
// app.use("/api/xauusd", xauusd_Routes)
// app.use("/api/btcusd", btcusdRoutes)
app.use("/api/banners", bannerRoutes);
app.use("/api/carousel", carouselRoutes);
app.use("/api/binance", binanceRoutes);
app.use("/api/transfer", internalTransferRoutes);
app.use("/api/mobile/v1", mobileRoutes);
// ✅ TradingView UDF endpoints for the mobile WebView chart
// Public (no auth middleware) — JWT validated per-request inside the router
app.use("/api/mobile/v1/tv", mobileTvDatafeedRoutes);
app.use("/api/admin-users-action", adminUserRoutes);
app.use("/api/competitions", competitionRoutes);
app.use("/api/competition", competitionLeaderboard);
app.use("/api/competition-email", competitionEmailRoutes);
app.use("/api/chart", chartRoutes);
app.use("/api/chart", chartHistoryRoutes);

app.use("/api/announcement", announcementRoute);
app.use("/api/notifications", notificationRoutes);

app.use("/api/withdraw-limit", withdrawLimitRoutes);
// Historical API route
// app.use("/api/history", historyRoute);

app.use("/api/market", marketRoutes);
app.use("/api/tv", tvDatafeedRoutes);
app.use("/api/market-insight", marketInsightRoutes);
app.use("/api/market-insight/currency-strength", currencyStrengthRoutes);
app.use("/api/trading-accounts", deleteTradingAccountRoute);

// Serve uploaded files statically
app.use("/uploads", express.static(path.join(__dirname, "uploads")));

// ✅ PRODUCTION-GRADE: Global Error Handler
// This catches all unhandled errors from routes and provides a consistent JSON response
app.use((err, req, res, next) => {
  const status = err.status || 500;
  const message = err.message || "Internal Server Error";

  console.error(
    `[GlobalError] ${req.method} ${req.url} -> ${status}: ${message}`,
  );

  // Log stack trace only in development
  if (process.env.NODE_ENV !== "production" && err.stack) {
    console.error(err.stack);
  }

  res.status(status).json({
    success: false,
    message: message,
  });
});

// Health check endpoints
app.get("/", (req, res) => {
  res.json({ message: "HCF Invest API is running" });
});

// Health check for CI/CD pipeline
app.get("/api/health", (req, res) => {
  res.json({
    status: "healthy",
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
    version: "1.0.0",
  });
});

const PORT = process.env.PORT || 8080;
// httpServer.listen(PORT, () => {
//   console.log(`Server running on port ${PORT}`)
// })

httpServer.listen(PORT, "0.0.0.0", () => {
  console.log(`Server running on port ${PORT}`);

  // ✅ KYC Reminder Cron
  console.log("🚀 Starting KYC Reminder Cron...");
  startKYCPendingReminderCron();
  console.log("✅ KYC Reminder Cron Function Executed");

  console.log("🚀 Starting Compliance KYC Expiry Cron...");

  startComplianceMonitorCron();

  console.log("✅ Compliance Cron Started");

  console.log("🚀 Starting Risk Alert Cron...");
  startRiskAlertCron();
  console.log("✅ Risk Alert Cron Started");

  // Initialize Background Services (Non-blocking)
  (async () => {
    try {
      // Start Candle Engine
      await candleEngine.start(alltickApiService).catch((err) => {
        console.error("[CandleEngine] Failed to start:", err.message);
      });

      const isMaster =
        process.env.NODE_APP_INSTANCE === "0" || !process.env.NODE_APP_INSTANCE;
      const instanceId = process.env.NODE_APP_INSTANCE || 0;
      if (isMaster) {
        console.log(
          `[System] Instance ${instanceId} designated as Master. Starting singleton services...`,
        );
        // Start Sync Worker (Only on Master)
        syncWorker.start();
      } else {
        console.log(
          `[System] Instance ${instanceId} running in Slave mode (Real-time only).`,
        );
      }

      // 🛡️ Wait for MongoDB to be connected before starting DB-dependent services
      if (mongoose.connection.readyState !== 1) {
        console.log("[System] Waiting for MongoDB connection...");
        await new Promise((resolve) => {
          mongoose.connection.once("connected", resolve);
        });
      }

      console.log("[System] Initializing background services...");

      // 1. Initialize IB Rule Engine (In-memory cache)
      if (ibRuleEngine && typeof ibRuleEngine.init === "function") {
        await ibRuleEngine
          .init()
          .catch((err) =>
            console.error("IB Rule Engine Init failed:", err.message),
          );
      }

      // 2. Start IB Commission Worker (Async Queue)
      if (typeof startIBWorker === "function") {
        try {
          startIBWorker(io);
        } catch (err) {
          console.error("[IB Worker] Could not start worker:", err.message);
          if (err.message.includes("Redis version")) {
            console.warn(
              "[IB Worker] WARNING: Redis 5.0+ is required for BullMQ.",
            );
          }
        }
      }

      // 3. Initialize Market Insight Jobs
      if (typeof initMarketInsightCron === "function") {
        initMarketInsightCron(io);
      }

      // 4. Initialize Competition Status Cron
      if (typeof initCompetitionCron === "function") {
        initCompetitionCron(io);
      }

      // 5. Initialize Maintenance Jobs (Swaps, Daily Resets)
      const { initMaintenanceJobs } =
        await import("./utils/maintenanceJobs.js");
      initMaintenanceJobs();

      console.log("[System] Background services initialized.");
    } catch (err) {
      console.error("[System] Background service initialization error:", err);
    }
  })();

  // Start XAUUSD streamer

  // streamer.startXAUUSDStreamer().catch(err => console.error('[Streamer] Error:', err.message));

  // Start background sync WITHOUT BLOCKING - fire and forget
  if (ENABLE_PERIODIC_HISTORY_SYNC) {
    console.log("[StorageService] Starting background sync (non-blocking)...");
    storageService
      .syncAllSymbols()
      .then(() => console.log("[StorageService] Initial sync completed"))
      .catch((err) =>
        console.error("[StorageService] Initial sync failed:", err.message),
      );

    // Schedule periodic syncs every 5 minutes
    syncInterval = setInterval(
      () => {
        storageService
          .syncAllSymbols()
          .catch((err) =>
            console.error(
              "[StorageService] Periodic sync failed:",
              err.message,
            ),
          );
      },
      5 * 60 * 1000,
    );
  } else {
    console.log(
      "[StorageService] Periodic history sync is disabled by feature flag.",
    );
  }
});
