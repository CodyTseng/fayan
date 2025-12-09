package calculator

import (
	"context"
	"database/sql"
	"log"
	"math"
	"sort"
	"time"

	"fayan/database"
)

type scoreWithID struct {
	id    int32
	score float64
}

// Calculate performs both PageRank and TrustRank algorithms on the stored connections and updates scores.
// - PageRank: measures global influence/popularity
// - TrustRank: measures trust propagated from seed nodes
func Calculate(db *sql.DB, seedPubkeys []string) error {
	// 1. Map string IDs to integer IDs to save memory
	pubkeyToID := make(map[string]int32)
	idToPubkey := make([]string, 0)

	type edge struct {
		source int32
		target int32
	}
	edges := make([]edge, 0, 1000) // Pre-allocate with estimated capacity

	getID := func(pubkey string) int32 {
		if id, ok := pubkeyToID[pubkey]; ok {
			return id
		}
		id := int32(len(idToPubkey))
		pubkeyToID[pubkey] = id
		idToPubkey = append(idToPubkey, pubkey)
		return id
	}

	// Stream connections and build edge list using a read-only transaction
	// Only consider connections seen in the last 30 days
	log.Println("   [INFO] Streaming connections from database...")
	connectionCount := 0

	// Only consider connections from the last 30 days
	cutoffTime := time.Now().UTC().AddDate(0, 0, -30)

	// Use a read-only transaction to avoid interfering with writes
	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}

	err = database.StreamConnectionsInTx(tx, func(conn database.Connection) error {
		sourceID := getID(conn.Source)
		targetID := getID(conn.Target)

		// Skip self-loops during streaming
		if sourceID != targetID {
			edges = append(edges, edge{source: sourceID, target: targetID})
		}
		connectionCount++
		return nil
	}, &cutoffTime)

	// Always close the transaction
	if txErr := tx.Rollback(); txErr != nil && err == nil {
		err = txErr
	}

	if err != nil {
		return err
	}

	numNodes := len(idToPubkey)
	if numNodes == 0 {
		log.Println("   [WARN] Graph is empty, skipping calculation")
		return nil
	}

	log.Printf("   [INFO] Processing %d nodes, %d connections", numNodes, connectionCount)

	// 2. Build seed node set for TrustRank
	seedSet := make(map[int32]bool)
	for _, pubkey := range seedPubkeys {
		if id, ok := pubkeyToID[pubkey]; ok {
			seedSet[id] = true
		}
	}
	log.Printf("   [INFO] Found %d seed nodes in graph (out of %d configured)", len(seedSet), len(seedPubkeys))

	// 3. Build the graph using slices (Adjacency List)
	// inLinks[i] contains the list of nodes that point to node i
	inLinks := make([][]int32, numNodes)
	// outDegree[i] contains the number of outgoing edges from node i
	outDegree := make([]int32, numNodes)

	for _, e := range edges {
		inLinks[e.target] = append(inLinks[e.target], e.source)
		outDegree[e.source]++
	}

	// Release edges memory after graph construction
	edges = nil

	// 4. Run both algorithms
	dampingFactor := 0.85
	tolerance := 1e-5
	maxIterations := 100

	// Run PageRank
	log.Println("   [INFO] Running PageRank...")
	pageScores := runPageRank(numNodes, inLinks, outDegree, dampingFactor, tolerance, maxIterations)

	// Run TrustRank (only if we have seed nodes)
	var trustScores []float64
	if len(seedSet) > 0 {
		log.Println("   [INFO] Running TrustRank...")
		trustScores = runTrustRank(numNodes, inLinks, outDegree, seedSet, dampingFactor, tolerance, maxIterations)
	} else {
		log.Println("   [WARN] No seed nodes found, skipping TrustRank")
		trustScores = make([]float64, numNodes)
	}

	// 5. Calculate scores
	scores := make([]float64, numNodes)
	for i := range numNodes {
		// Combine TrustRank and PageRank scores
		scores[i] = 0.7*trustScores[i] + 0.3*pageScores[i]
	}

	// 5. Calculate ranks based on scores
	rankList := make([]scoreWithID, numNodes)
	for i := range numNodes {
		rankList[i] = scoreWithID{id: int32(i), score: scores[i]}
	}

	sort.Slice(rankList, func(i, j int) bool {
		return rankList[i].score > rankList[j].score
	})

	// 6. Update database
	log.Printf("   [INFO] Updating %d scores in database...", numNodes)
	updatedCount := 0
	for rank, item := range rankList {
		pubkey := idToPubkey[item.id]
		if pubkey == "" {
			continue
		}
		followers := len(inLinks[item.id])
		following := outDegree[item.id]
		err := database.UpdatePubkey(db, pubkey, item.score, rank+1, trustScores[item.id], pageScores[item.id], followers, following)
		if err != nil {
			log.Printf("   [WARN] Error updating score for pubkey %s: %v", pubkey, err)
		} else {
			updatedCount++
		}
	}

	log.Printf("   [INFO] Updated %d/%d scores successfully", updatedCount, numNodes)

	return nil
}

// runPageRank performs the PageRank algorithm
func runPageRank(numNodes int, inLinks [][]int32, outDegree []int32, dampingFactor, tolerance float64, maxIterations int) []float64 {
	scores := make([]float64, numNodes)
	newScores := make([]float64, numNodes)

	// Initialize scores uniformly
	initialScore := 1.0 / float64(numNodes)
	for i := range scores {
		scores[i] = initialScore
	}

	for iter := range maxIterations {
		danglingSum := 0.0
		for i := range numNodes {
			if outDegree[i] == 0 {
				danglingSum += scores[i]
			}
		}

		diff := 0.0
		for i := range numNodes {
			incomingSum := 0.0
			for _, sourceID := range inLinks[i] {
				incomingSum += scores[sourceID] / float64(outDegree[sourceID])
			}

			// PageRank formula: (1-d)/N + d * (incomingSum + danglingSum/N)
			newScore := (1.0-dampingFactor)/float64(numNodes) + dampingFactor*(incomingSum+danglingSum/float64(numNodes))

			newScores[i] = newScore
			diff += math.Abs(newScore - scores[i])
		}

		copy(scores, newScores)

		if diff < tolerance {
			log.Printf("   [INFO] PageRank converged in %d iterations (diff: %.2e)", iter+1, diff)
			break
		}

		if (iter+1)%20 == 0 {
			log.Printf("   [INFO] PageRank iteration %d, diff: %.2e", iter+1, diff)
		}
	}

	return scores
}

// runTrustRank performs the TrustRank algorithm
func runTrustRank(numNodes int, inLinks [][]int32, outDegree []int32, seedSet map[int32]bool, dampingFactor, tolerance float64, maxIterations int) []float64 {
	scores := make([]float64, numNodes)
	newScores := make([]float64, numNodes)

	// Initialize scores: only seed nodes get initial trust
	initialSeedScore := 1.0 / float64(len(seedSet))
	for seedID := range seedSet {
		scores[seedID] = initialSeedScore
	}

	for iter := range maxIterations {
		diff := 0.0
		for i := range numNodes {
			incomingSum := 0.0
			for _, sourceID := range inLinks[i] {
				incomingSum += scores[sourceID] / float64(outDegree[sourceID])
			}

			// TrustRank formula: d * incomingSum + (1-d) * seedScore
			var seedBoost float64
			if seedSet[int32(i)] {
				seedBoost = (1.0 - dampingFactor) * initialSeedScore
			}
			newScore := dampingFactor*incomingSum + seedBoost

			newScores[i] = newScore
			diff += math.Abs(newScore - scores[i])
		}

		copy(scores, newScores)

		if diff < tolerance {
			log.Printf("   [INFO] TrustRank converged in %d iterations (diff: %.2e)", iter+1, diff)
			break
		}

		if (iter+1)%20 == 0 {
			log.Printf("   [INFO] TrustRank iteration %d, diff: %.2e", iter+1, diff)
		}
	}

	return scores
}
