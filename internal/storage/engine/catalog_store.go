package engine

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
)

const catalogMagic uint32 = 0x43415441 // "CATA"

type catalogDTO struct {
	DeviceID  string          `json:"device_id"`
	Metrics   map[string]bool `json:"metrics"`
	FirstSeen int64           `json:"first_seen"`
	LastSeen  int64           `json:"last_seen"`
}

// SaveCatalog atomically serializes the device catalog to disk with CRC32 verification.
func SaveCatalog(baseDir string, catalog map[string]*catalogItem) error {
	dtos := make([]catalogDTO, 0, len(catalog))
	for _, item := range catalog {
		dtos = append(dtos, catalogDTO{
			DeviceID:  item.deviceID,
			Metrics:   item.metrics,
			FirstSeen: item.firstSeen,
			LastSeen:  item.lastSeen,
		})
	}

	payload, err := json.Marshal(dtos)
	if err != nil {
		return fmt.Errorf("failed to marshal catalog: %w", err)
	}

	crc := crc32.ChecksumIEEE(payload)
	header := make([]byte, 12)
	binary.BigEndian.PutUint32(header[0:4], catalogMagic)
	binary.BigEndian.PutUint32(header[4:8], uint32(len(payload)))
	binary.BigEndian.PutUint32(header[8:12], crc)

	data := append(header, payload...)

	tmpPath := filepath.Join(baseDir, "catalog.bin.tmp")
	finalPath := CatalogPath(baseDir)

	if err := os.WriteFile(tmpPath, data, 0640); err != nil {
		return fmt.Errorf("failed to write temp catalog file: %w", err)
	}

	if err := os.Rename(tmpPath, finalPath); err != nil {
		return fmt.Errorf("failed to commit catalog file: %w", err)
	}

	return nil
}

// LoadCatalog reads and verifies the persistent device catalog from disk.
// Returns an empty map if the catalog file does not exist.
func LoadCatalog(baseDir string) (map[string]*catalogItem, error) {
	finalPath := CatalogPath(baseDir)
	data, err := os.ReadFile(finalPath)
	if err != nil {
		if os.IsNotExist(err) {
			return make(map[string]*catalogItem), nil
		}
		return nil, fmt.Errorf("failed to read catalog file: %w", err)
	}

	if len(data) < 12 {
		return nil, fmt.Errorf("%w: catalog file truncated", ErrCorruptWAL)
	}

	magic := binary.BigEndian.Uint32(data[0:4])
	if magic != catalogMagic {
		return nil, fmt.Errorf("%w: invalid catalog magic 0x%X", ErrInvalidMagic, magic)
	}

	payloadLen := binary.BigEndian.Uint32(data[4:8])
	expectedCRC := binary.BigEndian.Uint32(data[8:12])

	if uint32(len(data)-12) < payloadLen {
		return nil, fmt.Errorf("%w: catalog payload truncated", ErrCorruptWAL)
	}

	payload := data[12 : 12+payloadLen]
	actualCRC := crc32.ChecksumIEEE(payload)
	if actualCRC != expectedCRC {
		return nil, fmt.Errorf("%w: catalog CRC mismatch", ErrCorruptWAL)
	}

	var dtos []catalogDTO
	if err := json.Unmarshal(payload, &dtos); err != nil {
		return nil, fmt.Errorf("failed to unmarshal catalog payload: %w", err)
	}

	items := make(map[string]*catalogItem, len(dtos))
	for _, dto := range dtos {
		items[dto.DeviceID] = &catalogItem{
			deviceID:  dto.DeviceID,
			metrics:   dto.Metrics,
			firstSeen: dto.FirstSeen,
			lastSeen:  dto.LastSeen,
		}
	}

	return items, nil
}
