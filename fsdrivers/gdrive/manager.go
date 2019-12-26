// Package gdrive allows to access Google Drive files
package gdrive

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"

	"github.com/fclairamb/ftpserver/server/log"
)

// Config defines the parameter for the drive manager
type Config struct {
	OAuth2ConfigFile string // Auth config filename
	HTTPBase         string // Local URL server
}

// DriveManager handles all the google drive communications
type DriveManager struct {
	logger       log.Logger
	oAuth2Config *oauth2.Config
	config       *Config
}

// NewDriveManager instantiates a new GDrive driver
func NewDriveManager(config *Config, logger log.Logger) *DriveManager {
	mgr := &DriveManager{
		config: config,
		logger: logger,
	}

	return mgr
}

const (
	scopeUserInfoEmail   = "https://www.googleapis.com/auth/userinfo.email"
	scopeUserInfoProfile = "https://www.googleapis.com/auth/userinfo.profile"
	urlUserInfo          = "https://www.googleapis.com/oauth2/v1/userinfo?alt=json"
)

// Init initializes the DriveManager
func (mgr *DriveManager) Init() error {
	b, err := ioutil.ReadFile(mgr.config.OAuth2ConfigFile)
	if err != nil {
		mgr.logger.Error(
			"msg", "Unable to read client secret file",
			"oAuth2ConfigFile", mgr.config.OAuth2ConfigFile,
			"err", err,
		)
		return err
	}

	mgr.oAuth2Config, err = google.ConfigFromJSON(b, drive.DriveScope, scopeUserInfoProfile, scopeUserInfoEmail)
	if err != nil {
		mgr.logger.Error("msg", "Unable to parse client secret file to oAuth2Config", "err", err)
		return err
	}

	return nil
}

// nolint: unused
func (mgr *DriveManager) exchangeCodeToToken(ctx context.Context, code string) (*oauth2.Token, error) {
	return mgr.oAuth2Config.Exchange(ctx, code)
}

// nolint: unused
func (mgr *DriveManager) authCodeUrl() string {
	return mgr.authCodeUrlWithRedirect(mgr.config.HTTPBase + "/drive/oauth2callback")
}

// nolint: unused
func (mgr *DriveManager) authCodeUrlWithRedirect(redirectUrl string) string {
	authURL := mgr.oAuth2Config.AuthCodeURL(
		"state-Token",
		oauth2.AccessTypeOffline,
		oauth2.ApprovalForce,
		oauth2.SetAuthURLParam("redirect_uri", redirectUrl),
	)
	return authURL
}

// nolint: unused
func (mgr *DriveManager) newClientFromCode(code string) (*DriveClient, error) {
	ctx := context.Background()

	var token *oauth2.Token
	var err error

	if token, err = mgr.exchangeCodeToToken(ctx, code); err != nil {
		return nil, err
	}
	return mgr.newClientFromToken(ctx, token)
}

// nolint: unused
func (mgr *DriveManager) newClientFromToken(ctx context.Context, token *oauth2.Token) (*DriveClient, error) {
	client := mgr.oAuth2Config.Client(ctx, token)

	srv, err := drive.New(client)
	if err != nil {
		mgr.logger.Error("msg", "Unable to retrieve drive Client", "err", err)
		return nil, err
	}

	return &DriveClient{
		Token:   token,
		client:  client,
		service: srv,
		info:    nil,
	}, nil
}

// nolint: unused
func (mgr *DriveManager) newClientFromUser(userToken *oauth2.Token) (*drive.Service, error) {
	client := mgr.oAuth2Config.Client(context.Background(), userToken)
	mgr.logger.Info("msg", "NewClientFromUser", "user", userToken, "client", client)
	return drive.New(client)
}

type DriveClient struct {
	Token   *oauth2.Token          // Token is for OAuth2 token to use
	client  *http.Client           // HTTP Client
	service *drive.Service         // Drive client service
	info    map[string]interface{} // Information
	logger  log.Logger             // Logger
}

// nolint: unused
func (clt *DriveClient) getInfo() (map[string]interface{}, error) {
	if clt.info != nil {
		return clt.info, nil
	}

	var resp *http.Response
	var err error

	if resp, err = clt.client.Get(urlUserInfo); err != nil {
		return nil, err
	}

	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			clt.logger.Warn("msg", "Couldn't close stream", "err", errClose)
		}
	}()

	if resp.StatusCode != 200 {
		return nil, errors.New(fmt.Sprintf("%s returned the wrong status code: %d", urlUserInfo, resp.StatusCode))
	}

	output := make(map[string]interface{})

	if errDecode := json.NewDecoder(resp.Body).Decode(&output); errDecode != nil {
		clt.logger.Error("msg", "Problem parsing data", "err", errDecode)
		return nil, errDecode
	}

	clt.info = output
	return output, nil
}
