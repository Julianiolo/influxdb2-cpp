#ifndef __INFLUXDB2_CPP_H__
#define __INFLUXDB2_CPP_H__

#include <string>
#include <vector>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <set>
#include <exception>
#include <cstdbool>

#include <iostream>

#include <cpr/cpr.h>

#define IDB2CPP_ASSERT(x) if(!(x)) {fprintf(stderr,"Abort! \"%s\" @%s:%u\n", #x, __FILE__, __LINE__); abort();};

template<typename ... Args>
std::string format(const char* str, Args ... args) { // https://stackoverflow.com/a/26221725
    int size_i = std::snprintf(NULL, 0, str, args ...);
    if (size_i <= 0)
        throw std::runtime_error("error during string formatting");

    size_i++; // add size for null term

    char* buf = new char[size_i];

    std::snprintf(buf, size_i, str, args ...);

    std::string s = std::string(buf, buf + size_i - 1);
    delete[] buf;
    return s;
}

namespace idb2cpp {
    class ServerInfo {
    public:
        std::string url;
        std::string org;
        std::string bucket;
        std::string token;

    public:
        ServerInfo(const std::string& url, const std::string& org, const std::string& bucket, const std::string& token) :
            url(url), org(org), bucket(bucket), token(token) {

        }
    };

    class PostException : public std::runtime_error {
    public:
        cpr::Response resp;

        PostException(const cpr::Response& r) : std::runtime_error(format("Error posting data: %d", r.status_code)), resp(r) {

        }
    };

    auto query(const std::string& query, const ServerInfo& server_info) {
        return cpr::Post(
            cpr::Url{server_info.url + "/api/v2/query"},
            cpr::Header{{"Authorization", format("Token %s", server_info.token.c_str())}},
            cpr::Header{{"Accept", "application/csv"}},
            cpr::Header{{"Content-type", "application/vnd.flux"}},
            cpr::Parameters{{"org",server_info.org}},
            cpr::Body{query}
        );
    }

    template<typename FUNC>
    auto query_async(const std::string& query, const ServerInfo& server_info, const FUNC& func) {
        return cpr::PostCallback(func,
            cpr::Url{server_info.url + "/api/v2/query"}, 
            cpr::Header{{"Authorization", format("Token %s", server_info.token.c_str())}},
            cpr::Header{{"Accept", "application/csv"}},
            cpr::Header{{"Content-type", "application/vnd.flux"}},
            cpr::Parameters{{"org",server_info.org}},
            cpr::Body{query}
        );
    }

    class Builder {
    private:
        struct Write {
            std::string meas;
            bool meas_selected = false;
            std::vector<std::pair<std::string,std::string>> tags;
            struct FieldVal {
                std::string val;
                bool is_str = false;
            };
            std::vector<std::pair<std::string,FieldVal>> fields;
            uint64_t timestamp = -1;
        };

        std::vector<Write> writes;
        
    public:
        inline Builder() : writes(1) {

        }

        inline Builder& meas(const std::string& meas) {
            IDB2CPP_ASSERT(!writes.back().meas_selected);
            writes.back().meas_selected = true;

            writes.back().meas = meas;

            return *this;
        }

        inline Builder& tag(const std::string& key, const std::string& value) {
            writes.back().tags.push_back({key, value});
            return *this;
        }

        inline Builder& field(const std::string& key, const std::string& value, bool is_string=true) {
            writes.back().fields.push_back({key, {value, is_string}});
            return *this;
        }
        inline Builder& field(const std::string& key, uint64_t value) {
            writes.back().fields.push_back({key, {std::to_string(value)+"u"}});
            return *this;
        }
        inline Builder& field(const std::string& key, int64_t value) {
            writes.back().fields.push_back({key, {std::to_string(value)+"i"}});
            return *this;
        }
        inline Builder& field(const std::string& key, double value, const char* fmt = "%f") {
            std::string value_str = format(fmt, value);
            writes.back().fields.push_back({key, {value_str}});
            return *this;
        }
        inline Builder& field(const std::string& key, bool value) {
            writes.back().fields.push_back({key, {value?"T":"F"}});
            return *this;
        }

        inline Builder& timestamp(uint64_t unix_nanos) {
            writes.back().timestamp = unix_nanos;
            return *this;
        }

        inline cpr::Response post_http(const ServerInfo& server_info) {
            std::string line_str;
            for(size_t i = 0; i<writes.size(); i++) {
                line_str += construct_line_str(writes[i]);
            }

            return cpr::Post(
                cpr::Url{server_info.url + "/api/v2/write"}, cpr::Header{{"Authorization", format("Token %s", server_info.token)}},
                cpr::Parameters{{"bucket",server_info.bucket}, {"org",server_info.org}},
                cpr::Body{line_str}
            );
        }

        template<typename FUNC>
        inline auto post_http_async(const ServerInfo& server_info, const FUNC& func) {
            std::string line_str;
            for(size_t i = 0; i<writes.size(); i++) {
                line_str += construct_line_str(writes[i]);
            }

            return cpr::PostCallback(func,
                cpr::Url{server_info.url + "/api/v2/write"}, cpr::Header{{"Authorization", format("Token %s", server_info.token)}},
                cpr::Parameters{{"bucket",server_info.bucket}, {"org",server_info.org}},
                cpr::Body{line_str}
            );
        }
    
    private:
        inline static std::string construct_line_str(const Write& write) {
            IDB2CPP_ASSERT(write.meas_selected);
            IDB2CPP_ASSERT(write.fields.size() > 0);
            IDB2CPP_ASSERT(write.timestamp != (decltype(write.timestamp))-1);

            std::set<char> meas_esc({',',' '});
            std::set<char> tagkey_esc({',','=',' '});
            std::set<char> tagval_esc({',','=',' '});
            std::set<char> fieldkey_esc({',','=',' '});
            std::set<char> fieldval_esc({'"','\\'});

            std::stringstream out;
            out << escape_str(write.meas, meas_esc) << ',';

            for(size_t i = 0; i<write.tags.size(); i++) {
                out << escape_str(write.tags[i].first, tagkey_esc) 
                    << '=' << escape_str(write.tags[i].second, tagval_esc) << ',';
            }

            out << ' ';

            for(size_t i = 0; i<write.fields.size(); i++) {
                out << escape_str(write.fields[i].first, fieldkey_esc) 
                    << '=';

                if(write.fields[i].second.is_str) {
                    out << escape_str(write.fields[i].second.val, fieldval_esc) << ',';
                } else{
                    out << write.fields[i].second.val;
                }

                out << ',';
            }

            out << ' ';

            out << write.timestamp << '\n';
            return out.str();
        }

        inline static std::string escape_str(const std::string s, std::set<char> chars_to_escape) {
            std::string res;
            res.reserve(s.size());
            for(size_t i = 0; i<s.size(); i++) {
                char c = s[i];
                if(chars_to_escape.find(c) != chars_to_escape.end()) {
                    res += '\\';
                }
                res += c;
            }
            return res;
        }
    };
}

#endif