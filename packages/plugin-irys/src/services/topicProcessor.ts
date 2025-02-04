import { IAgentRuntime, Memory, Service, ServiceType, elizaLogger, IrysDataType, IrysMessageType, ModelClass, generateText } from "@elizaos/core";
import { IrysService } from "./irysService";

export class TopicProcessor extends Service {

    static serviceType = 'TOPIC_PROCESSOR' as ServiceType;
    private runtime: IAgentRuntime;
    private processedMessages = new Set<string>();
    private isProcessing = false;
    private irysService: IrysService;
    private cachedTopics = new Set<string>();
    private lastIrysCheck = 0;
    private transactionCache = new Map<string, {
        data: any,
        timestamp: number
    }>();
    private CACHE_DURATION = 5 * 60 * 1000; // 5 minutes
    private MAX_TRANSACTIONS = 5;
    private lastTransactionCache: string[] = [];
    private initialTopicsLoaded = false;
    private messageQueue: Memory[] = [];
    private BATCH_SIZE = 10;
    private processingTimeout: NodeJS.Timeout | null = null;

    private readonly genericWords = new Set([
        'time', 'held', 'here', 'what', 'when', 'where', 'who', 'why',
        'how', 'that', 'this', 'those', 'than', 'data', 'info', 'thing',
        'stuff', 'item', 'text', 'content', 'message', 'system', 'type'
    ]);

    async initialize(runtime: IAgentRuntime): Promise<void> {
        this.runtime = runtime;
        this.irysService = runtime.getService(ServiceType.IRYS) as IrysService;
        await this.loadExistingTopics();
        this.setupMessageHandler();
    }

    private setupMessageHandler(): void {
        const originalCreateMemory = this.runtime.messageManager.createMemory;
        this.runtime.messageManager.createMemory = async (memory: Memory) => {
            const result = await originalCreateMemory.call(this.runtime.messageManager, memory);
            await this.queueMessage(memory);
            return result;
        };
    }

    private async loadExistingTopics(): Promise<void> {
        //check cache if within duration
        try {
            if (Date.now() - this.lastIrysCheck < this.CACHE_DURATION) {
                return;
            }
        // Load character's existing topics only once
            if (!this.initialTopicsLoaded) {
                this.runtime.character.topics?.forEach(topic => {
                    this.cachedTopics.add(topic.toLowerCase());
                });
                this.initialTopicsLoaded = true;
            }
            // Get only new transactions since last check
            const timestamp = {
                from: Math.floor(this.lastIrysCheck / 1000),
                to: Math.floor(Date.now() / 1000)
            };

            const existingData = await this.irysService.getDataFromAnAgent(
                null,
                [{ name: "Service-Category", values: ["Topics"] }],
                timestamp
            );

            if (!existingData.success || !existingData.data) return;

            const recentTransactions = existingData.data.slice(-this.MAX_TRANSACTIONS);
            this.lastTransactionCache = recentTransactions.map(t => t.id);

            recentTransactions.forEach(item => {
                if (!item.data?.topics) return;
                item.data.topics.forEach(topic => {
                    const normalizedTopic = topic.toLowerCase();
                    this.cachedTopics.add(normalizedTopic);
                });
            });

            this.lastIrysCheck = Date.now();
            elizaLogger.debug("Current topic cache:", Array.from(this.cachedTopics));
        } catch (error) {
            elizaLogger.error("Error loading topics:", error);
        }
    }


    private async queueMessage(memory: Memory): Promise<void> {
        if (this.shouldSkipMessage(memory)) return;

        this.messageQueue.push(memory);
        this.processedMessages.add(memory.id);

        if (this.messageQueue.length >= this.BATCH_SIZE) {
            await this.processBatch();
        } else if (!this.processingTimeout) {
            this.processingTimeout = setTimeout(async () => {
                await this.processBatch();
                this.processingTimeout = null;
            }, 5000);
        }
    }

    private async processBatch(): Promise<void> {
        if (this.isProcessing || this.messageQueue.length === 0) return;

        try {
            this.isProcessing = true;

            if (Date.now() - this.lastIrysCheck > this.CACHE_DURATION) {
                await this.loadExistingTopics();
            }

            const messages = [...this.messageQueue];
            this.messageQueue = [];

            const newTopics = new Set<string>();

            for (const message of messages) {
                const topics = await this.extractTopicsWithAI(message.content.text);
                topics.forEach(topic => newTopics.add(topic.toLowerCase()));
            }

            const uniqueTopics = Array.from(newTopics)
                .filter(topic => !this.cachedTopics.has(topic));

            if (uniqueTopics.length > 0) {
                await this.storeNewTopics(uniqueTopics);
            }
        } finally {
            this.isProcessing = false;
        }
    }

    private isGenericWord(word: string): boolean {
        return this.genericWords.has(word.toLowerCase());
    }

    private isValidTopic(topic: string): boolean {
        if (!topic || typeof topic !== 'string') return false;

        const normalizedTopic = topic.toLowerCase().trim();
        if (normalizedTopic.length < 4 || normalizedTopic.length > 50) return false;
        if (this.isGenericWord(normalizedTopic)) return false;

        const validFormat = /^[a-z]+(_[a-z]+)*$/i.test(normalizedTopic);
        elizaLogger.debug(`Topic validation: ${normalizedTopic} - ${validFormat}`);

        return validFormat;
    }

    private async extractTopicsWithAI(text: string): Promise<string[]> {
        const prompt = `Extract knowledge domains or technical topics related to this text.
Example format: ["cryptocurrency", "finance", "blockchain"]
Only extract specific fields/domains, no names or general terms.
Text: "${text}"`;

        try {
            elizaLogger.info("Processing text for topics:", text);
            const response = await generateText({
                runtime: this.runtime,
                context: prompt,
                modelClass: ModelClass.SMALL
            });

            elizaLogger.debug("Raw AI response:", response);
            const cleanResponse = response.replace(/```json|\s*```/g, '').trim();

            try {
                const topics = JSON.parse(cleanResponse);
                if (!Array.isArray(topics)) {
                    elizaLogger.warn("Invalid topics format:", topics);
                    return [];
                }

                const validTopics = topics.filter(topic => this.isValidTopic(topic));
                elizaLogger.info("Extracted valid topics:", validTopics);
                return validTopics;
            } catch (parseError) {
                elizaLogger.error("JSON parse error:", parseError);
                return [];
            }
        } catch (error) {
            elizaLogger.error("Topic extraction error:", error);
            return [];
        }
    }

    private shouldSkipMessage(memory: Memory): boolean {
        if (this.processedMessages.has(memory.id) ||
            !memory?.content?.text ||
            memory.userId === this.runtime.agentId) return true;

        const codeIndicators = ['import ', 'class ', 'function ', '{', '//', '/*'];
        return codeIndicators.some(indicator => memory.content.text.includes(indicator));
    }

    private async storeNewTopics(topics: string[]): Promise<void> {
        const normalizedTopics = topics
            .map(t => t.toLowerCase())
            .filter((topic, index, self) => self.indexOf(topic) === index)
            .filter(topic => !this.cachedTopics.has(topic));

        if (normalizedTopics.length === 0) return;

        elizaLogger.info("Topic Analysis:", {
            existingCharacterTopics: this.runtime.character.topics || [],
            currentCachedTopics: Array.from(this.cachedTopics),
            incomingTopics: normalizedTopics
        });

        const result = await this.irysService.workerUploadDataOnIrys(
            { topics: normalizedTopics },
            IrysDataType.OTHER,
            IrysMessageType.DATA_STORAGE,
            ["Topics"],
            ["Character Development"]
        );

        if (result.success) {
            const transactionId = result.url?.split('/').pop();
            elizaLogger.info("Topic Upload Transaction:", {
                transactionId,
                url: result.url,
                topics: normalizedTopics
            });
            normalizedTopics.forEach(topic => {
                this.cachedTopics.add(topic);
                if (!this.runtime.character.topics) {
                    this.runtime.character.topics = [];
                }
                this.runtime.character.topics.push(topic);
            });

            elizaLogger.info("Topic Update:", {
                addedTopics: normalizedTopics,
                updatedTopicList: this.runtime.character.topics
            });
        }
    }
}