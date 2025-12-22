package ranking

import (
	"log"
	"math"
	"sort"
	"time"

	"fayan/internal/models"
	"fayan/internal/repository"
)

type scoreWithID struct {
	id    int32
	score float64
}

// Calculator handles PageRank and TrustRank calculations
type Calculator struct {
	repo        *repository.Repository
	seedPubkeys []string
}

// NewCalculator creates a new Calculator instance
func NewCalculator(repo *repository.Repository, seedPubkeys []string) *Calculator {
	return &Calculator{
		repo:        repo,
		seedPubkeys: seedPubkeys,
	}
}

// Calculate performs both PageRank and TrustRank algorithms on the stored connections and updates scores.
func (c *Calculator) Calculate() error {
	pubkeyToID := make(map[string]int32)
	idToPubkey := make([]string, 0)

	type edge struct {
		source int32
		target int32
	}
	edges := make([]edge, 0, 1000)

	getID := func(pubkey string) int32 {
		if id, ok := pubkeyToID[pubkey]; ok {
			return id
		}
		id := int32(len(idToPubkey))
		pubkeyToID[pubkey] = id
		idToPubkey = append(idToPubkey, pubkey)
		return id
	}

	log.Println("   [INFO] Streaming connections from database...")
	connectionCount := 0

	cutoffTime := time.Now().UTC().AddDate(0, 0, -30)

	err := c.repo.StreamConnectionsInTx(func(conn models.Connection) error {
		sourceID := getID(conn.Source)
		targetID := getID(conn.Target)

		if sourceID != targetID {
			edges = append(edges, edge{source: sourceID, target: targetID})
		}
		connectionCount++
		return nil
	}, &cutoffTime)

	if err != nil {
		return err
	}

	numNodes := len(idToPubkey)
	if numNodes == 0 {
		log.Println("   [WARN] Graph is empty, skipping calculation")
		return nil
	}

	log.Printf("   [INFO] Processing %d nodes, %d connections", numNodes, connectionCount)

	// Build seed node set for TrustRank
	seedSet := make(map[int32]bool)
	for _, pubkey := range c.seedPubkeys {
		if id, ok := pubkeyToID[pubkey]; ok {
			seedSet[id] = true
		}
	}
	log.Printf("   [INFO] Found %d seed nodes in graph (out of %d configured)", len(seedSet), len(c.seedPubkeys))

	// Build the graph using slices (Adjacency List)
	inLinks := make([][]int32, numNodes)
	outDegree := make([]int32, numNodes)

	for _, e := range edges {
		inLinks[e.target] = append(inLinks[e.target], e.source)
		outDegree[e.source]++
	}

	edges = nil // Release memory

	dampingFactor := 0.85
	tolerance := 1e-5
	maxIterations := 100

	// Run PageRank
	log.Println("   [INFO] Running PageRank...")
	pageScores := c.runPageRank(numNodes, inLinks, outDegree, dampingFactor, tolerance, maxIterations)

	// Run TrustRank
	var trustScores []float64
	if len(seedSet) > 0 {
		log.Println("   [INFO] Running TrustRank...")
		trustScores = c.runTrustRank(numNodes, inLinks, outDegree, seedSet, dampingFactor, tolerance, maxIterations)
	} else {
		log.Println("   [WARN] No seed nodes found, skipping TrustRank")
		trustScores = make([]float64, numNodes)
	}

	// Calculate combined scores
	scores := make([]float64, numNodes)
	for i := range numNodes {
		scores[i] = 0.7*trustScores[i] + 0.3*pageScores[i]
	}

	// Calculate ranks based on scores
	rankList := make([]scoreWithID, numNodes)
	for i := range numNodes {
		rankList[i] = scoreWithID{id: int32(i), score: scores[i]}
	}

	sort.Slice(rankList, func(i, j int) bool {
		return rankList[i].score > rankList[j].score
	})

	// Update database using batch updates to reduce WAL growth
	log.Printf("   [INFO] Updating %d scores in database...", numNodes)

	const batchSize = 1000
	updates := make([]repository.PubkeyUpdate, 0, batchSize)
	updatedCount := 0

	for rank, item := range rankList {
		pubkey := idToPubkey[item.id]
		if pubkey == "" {
			continue
		}

		updates = append(updates, repository.PubkeyUpdate{
			Pubkey:     pubkey,
			Score:      scores[item.id],
			Rank:       rank + 1,
			TrustScore: trustScores[item.id],
			PageScore:  pageScores[item.id],
			Followers:  len(inLinks[item.id]),
			Following:  outDegree[item.id],
		})

		// Batch update when we reach batchSize
		if len(updates) >= batchSize {
			if err := c.repo.BatchUpdatePubkeys(updates); err != nil {
				log.Printf("   [WARN] Batch update failed: %v", err)
			} else {
				updatedCount += len(updates)
			}
			updates = updates[:0]
		}
	}

	// Update remaining items
	if len(updates) > 0 {
		if err := c.repo.BatchUpdatePubkeys(updates); err != nil {
			log.Printf("   [WARN] Final batch update failed: %v", err)
		} else {
			updatedCount += len(updates)
		}
	}

	log.Printf("   [INFO] Updated %d/%d pubkeys", updatedCount, numNodes)

	// Force WAL checkpoint after large batch operation
	log.Println("   [INFO] Running WAL checkpoint...")
	c.repo.Checkpoint()

	return nil
}

// runPageRank executes the PageRank algorithm
func (c *Calculator) runPageRank(numNodes int, inLinks [][]int32, outDegree []int32, damping, tolerance float64, maxIterations int) []float64 {
	scores := make([]float64, numNodes)
	newScores := make([]float64, numNodes)
	initialScore := 1.0 / float64(numNodes)

	for i := range scores {
		scores[i] = initialScore
	}

	for iter := 0; iter < maxIterations; iter++ {
		danglingSum := 0.0
		for i := range numNodes {
			if outDegree[i] == 0 {
				danglingSum += scores[i]
			}
		}

		for i := range numNodes {
			sum := 0.0
			for _, j := range inLinks[i] {
				sum += scores[j] / float64(outDegree[j])
			}
			newScores[i] = (1-damping)/float64(numNodes) + damping*(sum+danglingSum/float64(numNodes))
		}

		// Check convergence
		diff := 0.0
		for i := range numNodes {
			diff += math.Abs(newScores[i] - scores[i])
		}

		scores, newScores = newScores, scores

		if diff < tolerance {
			log.Printf("   [INFO] PageRank converged after %d iterations", iter+1)
			break
		}
	}

	return scores
}

// runTrustRank executes the TrustRank algorithm
func (c *Calculator) runTrustRank(numNodes int, inLinks [][]int32, outDegree []int32, seedSet map[int32]bool, damping, tolerance float64, maxIterations int) []float64 {
	scores := make([]float64, numNodes)
	newScores := make([]float64, numNodes)

	// Initialize seed nodes with equal trust
	seedScore := 1.0 / float64(len(seedSet))
	for seedID := range seedSet {
		scores[seedID] = seedScore
	}

	for iter := 0; iter < maxIterations; iter++ {
		danglingSum := 0.0
		for i := range numNodes {
			if outDegree[i] == 0 {
				danglingSum += scores[i]
			}
		}

		for i := range numNodes {
			sum := 0.0
			for _, j := range inLinks[i] {
				sum += scores[j] / float64(outDegree[j])
			}

			teleport := 0.0
			if seedSet[int32(i)] {
				teleport = seedScore
			}

			newScores[i] = (1-damping)*teleport + damping*(sum+danglingSum*seedScore)
		}

		diff := 0.0
		for i := range numNodes {
			diff += math.Abs(newScores[i] - scores[i])
		}

		scores, newScores = newScores, scores

		if diff < tolerance {
			log.Printf("   [INFO] TrustRank converged after %d iterations", iter+1)
			break
		}
	}

	return scores
}
