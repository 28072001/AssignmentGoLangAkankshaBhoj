package main
import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
)

// Transaction represents a single transaction.
type Transaction struct {
	ID       string  `json:"id"`
	Value    float64 `json:"val"`
	Version  float64 `json:"ver"`
	Valid    bool    `json:"valid"`
	Hash     string  `json:"hash"`
	HashDone bool    `json:"-"`
}

// BlockStatus represents the status of a block.
type BlockStatus string

const (
	Committed BlockStatus = "committed"
	Pending   BlockStatus = "pending"
)

// Block represents a block in the blockchain.
type Block struct {
	BlockNumber  int           `json:"blockNumber"`
	PreviousHash string        `json:"prevBlockHash"`
	Transactions []Transaction `json:"txns"`
	Timestamp    int64         `json:"timestamp"`
	Status       BlockStatus   `json:"blockStatus"`
}

// BlockChain represents the blockchain.
type BlockChain struct {
	Blocks   []Block        `json:"blocks"`
	FileName string         `json:"-"`
	Mutex    sync.RWMutex   `json:"-"`
	DB       *leveldb.DB    `json:"-"`
	HashChan chan *Block    `json:"-"`
	WriteChan chan *Block   `json:"-"`
	DoneChan  chan struct{} `json:"-"`
}

// NewBlockChain creates a new blockchain instance.
func NewBlockChain(fileName string, db *leveldb.DB, hashChanSize, writeChanSize int) *BlockChain {
	return &BlockChain{
		Blocks:    []Block{},
		FileName:  fileName,
		Mutex:     sync.RWMutex{},
		DB:        db,
		HashChan:  make(chan *Block, hashChanSize),
		WriteChan: make(chan *Block, writeChanSize),
		DoneChan:  make(chan struct{}),
	}
}

// Start starts the block processing and writing to the file.
func (bc *BlockChain) Start() {
	go bc.processBlocks()
	go bc.writeBlocksToFile()
}

// processBlocks processes the blocks, calculates the hash of each transaction concurrently, and updates the block.
func (bc *BlockChain) processBlocks() {
	for block := range bc.HashChan {
		startTime := time.Now()

		var wg sync.WaitGroup
		for i := range block.Transactions {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				txn := &block.Transactions[i]
				txn.Hash = calculateHash(txn.ID, txn.Value, txn.Version)
				txn.HashDone = true

				// Validate version
				if txn.Version == getVersionFromLevelDB(bc.DB, txn.ID) {
					txn.Valid = true
				}
			}(i)
		}
		wg.Wait()

		block.Status = Committed
		bc.WriteChan <- block

		processingTime := time.Since(startTime)
		log.Printf("Block %d processing time: %v\n", block.BlockNumber, processingTime)
	}
	close(bc.WriteChan)
}

// writeBlocksToFile writes the blocks to the file.
func (bc *BlockChain) writeBlocksToFile() {
	file, err := os.OpenFile(bc.FileName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		log.Fatal("Error opening file:", err)
	}
	defer file.Close()

	for block := range bc.WriteChan {
		bc.Mutex.Lock()
		bc.Blocks = append(bc.Blocks, *block)
		bc.Mutex.Unlock()

		blockJSON, err := json.Marshal(block)
		if err != nil {
			log.Println("Error marshaling block to JSON:", err)
			continue
		}

		if _, err := file.Write(blockJSON); err != nil {
			log.Println("Error writing block to file:", err)
		}
	}

	bc.DoneChan <- struct{}{}
}

// getVersionFromLevelDB retrieves the version of a transaction from the LevelDB.
func getVersionFromLevelDB(db *leveldb.DB, key string) float64 {
	value, err := db.Get([]byte(key), nil)
	if err != nil {
		log.Println("Error retrieving value from LevelDB:", err)
		return 0
	}

	var txn Transaction
	err = json.Unmarshal(value, &txn)
	if err != nil {
		log.Println("Error unmarshaling stored transaction:", err)
		return 0
	}

	return txn.Version
}

// calculateHash calculates the hash of a transaction based on its properties.
func calculateHash(id string, value float64, version float64) string {
	return fmt.Sprintf("%s-%f-%f", id, value, version)
}

func main() {
	// Open LevelDB instance
	db, err := leveldb.OpenFile("data", nil)
	if err != nil {
		log.Fatal("Error opening LevelDB:", err)
	}
	defer db.Close()

	// Setup LevelDB entries
	for i := 1; i <= 1000; i++ {
		key := fmt.Sprintf("SIM%d", i)
		value := fmt.Sprintf(`{"val": %d, "ver": 1.0}`, i)
		err = db.Put([]byte(key), []byte(value), nil)
		if err != nil {
			log.Println("Error putting value into LevelDB:", err)
		}
	}

	// Create a new blockchain
	blockChain := NewBlockChain("ledger.txt", db, 100, 100)
	blockChain.Start()

	// Process input transactions
	inputTxns := []map[string]Transaction{
		{"SIM1": {ID: "SIM1", Value: 2.0, Version: 1.0}},
		{"SIM2": {ID: "SIM2", Value: 3.0, Version: 1.0}},
		{"SIM3": {ID: "SIM3", Value: 4.0, Version: 2.0}},
		{"SIM1": {ID: "SIM4", Value: 2.0, Version: 1.0}},
	}

	block := &Block{
		BlockNumber:  1,
		PreviousHash: "0xabc123",
		Transactions: []Transaction{},
		Timestamp:    time.Now().Unix(),
		Status:       Pending,
	}

	for _, txns := range inputTxns {
		for _, txn := range txns {
			block.Transactions = append(block.Transactions, txn)
		}
	}

	blockChain.HashChan <- block

	// Wait for block processing and writing to finish
	close(blockChain.HashChan)
	<-blockChain.DoneChan

	// Fetch block details by block number
	blockNumber := 1
	blockDetails := blockChain.GetBlockDetailsByNumber(blockNumber)
	fmt.Printf("Block %d details: %v\n", blockNumber, blockDetails)

	// Fetch details of all blocks
	allBlockDetails := blockChain.GetAllBlockDetails()
	fmt.Println("All block details:")
	for _, details := range allBlockDetails {
		fmt.Println(details)
	}
}

// GetBlockDetailsByNumber fetches the details of a block by its block number.
func (bc *BlockChain) GetBlockDetailsByNumber(blockNumber int) *Block {
	bc.Mutex.RLock()
	defer bc.Mutex.RUnlock()

	for _, block := range bc.Blocks {
		if block.BlockNumber == blockNumber {
			return &block
		}
	}

	return nil
}

// GetAllBlockDetails fetches the details of all blocks in the blockchain.
func (bc *BlockChain) GetAllBlockDetails() []Block {
	bc.Mutex.RLock()
	defer bc.Mutex.RUnlock()

	return bc.Blocks
}