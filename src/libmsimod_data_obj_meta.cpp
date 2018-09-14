// =-=-=-=-=-=-=-
// irods includes
#include "irods_error.hpp"
#include "irods_ms_plugin.hpp"
#include "irods_kvp_string_parser.hpp"
#include "irods_resource_manager.hpp"

#include "rsModDataObjMeta.hpp"
#include "rcMisc.h"

// =-=-=-=-=-=-=-
// stl includes
#include <string>

extern irods::resource_manager resc_mgr;


/* Possible Keywords
 * replNum,       dataType,     dataSize,
 * rescName,      filePath,     dataOwner,
 * dataOwnerZone, replStatus,   chksum,
 * dataExpiry,    dataComments, dataCreate,
 * dataModify,    dataMode,     resc_hier,
 * rescId
 */

int msimod_data_obj_meta(
    msParam_t*      _logical_path_str,
    msParam_t*      _resc_id_int,
    msParam_t*      _reg_param_str,
    ruleExecInfo_t* _rei ) {

    dataObjInfo_t obj_info;
    memset(&obj_info, 0, sizeof(obj_info));

    char *logical_path = parseMspForStr(_logical_path_str);
    if(!logical_path) {
        std::cout << "First parameter is NULL" << std::endl;
        return SYS_INVALID_INPUT_PARAM;
    }

    char* ret_str = rstrcpy(
                        obj_info.objPath,
                        logical_path,
                        MAX_NAME_LEN); 
    if(!ret_str) {
        rodsLog(
            LOG_ERROR,
            "%s:%d - failed to copy logical_path to obj_info.objPath",
            __FUNCTION__,
            __LINE__);
        return SYS_INVALID_INPUT_PARAM;
    }

    int leaf_id = parseMspForPosInt(_resc_id_int);
    if(leaf_id < 0) {
        std::cout << "Second parameter is invalid" << std::endl;
        return SYS_INVALID_INPUT_PARAM;
    }

    std::string resc_hier;
    irods::error ret = resc_mgr.leaf_id_to_hier(leaf_id, resc_hier);
    if(!ret.ok()) {
        irods::log(ret);
        return ret.code();
    }

    obj_info.rescId = leaf_id;

    ret_str = rstrcpy(
                 obj_info.rescHier,
                 resc_hier.c_str(),
                 MAX_NAME_LEN);
    if(!ret_str) {
        rodsLog(
            LOG_ERROR,
            "%s:%d - failed to copy resc_hier to obj_info.rescHier",
            __FUNCTION__,
            __LINE__);
        return SYS_INVALID_INPUT_PARAM;
    }

    char *reg_params = parseMspForStr(_reg_param_str);
    if(!reg_params) {
        std::cout << "Third parameter is NULL" << std::endl;
        return SYS_INVALID_INPUT_PARAM;
    }

    keyValPair_t key_val_pairs;
    memset(
        &key_val_pairs, 
        0, sizeof(key_val_pairs));

    irods::kvp_map_t tokens;
    irods::error err = irods::parse_kvp_string(
                           reg_params,
                           tokens);
    if(!err.ok()) {
        irods::log(err);
        return err.code();
    }

    for(auto& kv : tokens) {
        addKeyVal(
            &key_val_pairs,
            kv.first.c_str(),
            kv.second.c_str());
    }

    modDataObjMeta_t mod_obj_meta;
    mod_obj_meta.dataObjInfo = &obj_info;
    mod_obj_meta.regParam    = &key_val_pairs;
    int status = rsModDataObjMeta(
                 _rei->rsComm,
                 &mod_obj_meta);
    if(status < 0) {
        rodsLog(
            LOG_ERROR,
            "%s:%d - rsModDataObjMeta failed for lp [%s] rp [%s]",
            __FUNCTION__,
            __LINE__,
            logical_path,
            reg_params);
    }

    rodsLog(
        LOG_NOTICE,
        "XXXX - %s:%d - rsModDataObjMeta for lp [%s] rp [%s]",
        __FUNCTION__,
        __LINE__,
        logical_path,
        reg_params);

    return status;

} // msimod_data_obj_meta

extern "C"
irods::ms_table_entry* plugin_factory() {
    irods::ms_table_entry* msvc = new irods::ms_table_entry(3);
    msvc->add_operation<
        msParam_t*,
        msParam_t*,
        msParam_t*,
        ruleExecInfo_t*>("msimod_data_obj_meta",
                         std::function<int(
                             msParam_t*,
                             msParam_t*,
                             msParam_t*,
                             ruleExecInfo_t*)>(msimod_data_obj_meta));
    return msvc;
}

