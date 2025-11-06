package dal

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type UserDao struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Address   string    `json:"address"`
	Password  string    `json:"-"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

func GetUserDao(name, address, password string) *UserDao {
	rqId, _ := uuid.NewRandom()
	rqIdStr := rqId.String()

	return &UserDao{
		ID:        rqIdStr,
		Name:      name,
		Address:   address,
		Password:  password,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
}

func (u *UserDao) ToAvroNative() map[string]interface{} {
	return map[string]interface{}{
		"id":        u.ID,
		"name":      u.Name,
		"address":   u.Address,
		"password":  u.Password,
		"createdAt": u.CreatedAt.Format(time.RFC3339),
		"updatedAt": u.UpdatedAt.Format(time.RFC3339),
	}
}

func (u *UserDao) ToBytes() []byte {
	d, _ := json.Marshal(u)
	return d
}
