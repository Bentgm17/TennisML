import pandas as pd


class Transformation():

    def __init__(self,df):
        self.df=df
        self.new_df=pd.DataFrame([])
    
    def __call__(self):
        self.copy_match_id(),self.compute_ace(),self.compute_df(),self.compute_total_points(),self.compute_points_won_on_serve()
        self.compute_points_won_on_first_serve(),self.compute_points_won_on_second_serve(),self.compute_break_points_saved()
        self.compute_return_points_won_on_return_first_serve(),self.compute_return_points_won_on_return_second_serve()
        self.compute_break_points_won(),self.compute_return_points_won(),self.compute_point_dominance()
        self.compute_numbers_of_first_serve_made(),self.compute_aces_per_game()
        self.compute_df_per_game(),self.compute_aces_per_set(),self.compute_df_per_set()
        self.compute_df_per_second_serve(),self.compute_aces_per_df(),self.compute_first_serve_efficiency()
        self.compute_serve_ip_efficiency(),self.compute_points_lost_per_serv_game(),self.compute_break_points_faced_per_game()
        self.compute_break_points_faced_per_set(),self.compute_service_games_won(),self.compute_sv_games_lost_per_set()
        self.compute_ip_return_points(),self.compute_points_won_per_return_game(),self.compute_bp_per_game()
        self.compute_bp_per_set(),self.compute_return_games_won(),self.compute_return_games_won_per_set()
        self.compute_games_won(),self.compute_game_dominance(),self.compute_break_ratio()

    def copy_match_id(self):
        self.new_df['match_id']=self.df['match_id']

    def compute_ace(self):
        self.new_df['w_ace'],self.new_df['l_ace']=self.df['w_ace']/self.df['w_sv_pt'],self.df['l_ace']/self.df['l_sv_pt']

    def compute_df(self):
        self.new_df['w_df'],self.new_df['l_df']=self.df['w_df']/self.df['w_sv_pt'],self.df['l_df']/self.df['l_sv_pt']

    def compute_total_points(self):
        self.new_df['w_tt_p']=((self.df['w_1st_won']+self.df['w_2nd_won'])+(self.df['l_sv_pt']-(self.df['l_1st_won']+self.df['l_2nd_won'])))/(self.df['l_sv_pt']+self.df['w_sv_pt'])

    def compute_points_won_on_serve(self):
        self.new_df['w_sv_w'],self.new_df['l_sv_w']=(self.df['w_1st_won']+self.df['w_2nd_won'])/self.df['w_sv_pt'],(self.df['l_1st_won']+self.df['l_2nd_won'])/self.df['l_sv_pt']

    def compute_points_won_on_first_serve(self):
        self.new_df['w_1st_w'],self.new_df['l_1st_w']=self.df['w_1st_won']/self.df['w_1st_in'],self.df['l_1st_won']/self.df['l_1st_in']

    def compute_points_won_on_second_serve(self):
        self.new_df['w_2nd_w'],self.new_df['l_2nd_w']=self.df['w_2nd_won']/(self.df['w_sv_pt']-self.df['w_1st_in']),self.df['l_2nd_won']/(self.df['l_sv_pt']-self.df['l_1st_in'])

    def compute_break_points_saved(self):
        self.new_df['w_bp_save'],self.new_df['l_bp_save']=self.df['w_bp_sv']/self.df['w_bp_fc'],self.df['l_bp_sv']/self.df['l_bp_fc']

    def compute_return_points_won_on_return_first_serve(self):
        self.new_df['w_r_1st_w'],self.new_df['l_r_1st_w']=1-self.new_df['l_1st_w'],1-self.new_df['w_1st_w']

    def compute_return_points_won_on_return_second_serve(self):
        self.new_df['w_r_1st_w'],self.new_df['l_r_1st_w']=1-self.new_df['l_1st_w'],1-self.new_df['w_1st_w']

    def compute_break_points_won(self):
        self.new_df['w_bp_w'],self.new_df['l_bp_w']=1-self.new_df['l_bp_save'],1-self.new_df['w_bp_save']

    def compute_return_points_won(self):
        self.new_df['w_rp_w'],self.new_df['l_rp_w']=1-self.new_df['l_sv_w'],1-self.new_df['w_sv_w']

    def compute_point_dominance(self):
        self.new_df['w_p_dominance'],self.new_df['l_p_dominance']=self.new_df['w_rp_w']/self.new_df['l_rp_w'],self.new_df['l_rp_w']/self.new_df['w_rp_w']

    def compute_numbers_of_first_serve_made(self):
        self.new_df['w_1st_in'],self.new_df['l_1st_in']=self.df['w_1st_in']/self.df['w_sv_pt'],self.df['l_1st_in']/self.df['l_sv_pt']

    def compute_aces_per_game(self):
        self.new_df['w_ace_game'],self.new_df['l_ace_game']=self.df['w_ace']/self.df['w_sv_gms'],self.df['l_ace']/self.df['l_sv_gms']

    def compute_df_per_game(self):
        self.new_df['w_df_game'],self.new_df['l_df_game']=self.df['w_df']/self.df['w_sv_gms'],self.df['l_df']/self.df['l_sv_gms']

    def compute_aces_per_set(self):
        self.new_df['w_ace_set'],self.new_df['l_ace_set']=self.df['w_ace']/(self.df['w_sets']+self.df['l_sets']),self.df['l_ace']/(self.df['w_sets']+self.df['l_sets'])

    def compute_df_per_set(self):
        self.new_df['w_df_set'],self.new_df['l_df_set']=self.df['w_df']/(self.df['w_sets']+self.df['l_sets']),self.df['l_df']/(self.df['w_sets']+self.df['l_sets'])

    def compute_df_per_second_serve(self):
        self.new_df['w_df_2nd_sv'],self.new_df['l_df_2nd_sv']=self.df['w_df']/(self.df['w_sv_pt']-self.df['w_1st_in']),self.df['l_df']/(self.df['l_sv_pt']-self.df['l_1st_in'])

    def compute_aces_per_df(self):
        self.new_df['w_ace_df_ratio'],self.new_df['l_ace_df_ratio']=self.df['w_ace']/self.df['w_df'],self.df['l_ace']/self.df['l_df']

    def compute_first_serve_efficiency(self):
        self.new_df['w_1st_sv_eff'],self.new_df['l_1st_sv_eff']=self.new_df['w_1st_w']/self.new_df['w_2nd_w'],self.new_df['l_1st_w']/self.new_df['l_2nd_w']

    def compute_serve_ip_efficiency(self):
        self.new_df['w_sv_ip_w'],self.new_df['l_sv_ip_w']=(self.df['w_1st_won']+self.df['w_2nd_won']-self.df['w_ace']-self.df['w_df'])/(self.df['w_sv_pt']-self.df['w_ace']-self.df['w_df']),(self.df['l_1st_won']+self.df['l_2nd_won']-self.df['l_ace']-self.df['l_df'])/(self.df['l_sv_pt']-self.df['l_ace']-self.df['l_df'])

    def compute_points_lost_per_serv_game(self):
        self.new_df['w_pl_game'],self.new_df['l_pl_game']=(self.df['w_sv_pt']-self.df['w_1st_won']-self.df['w_2nd_won'])/self.df['w_sv_gms'],(self.df['l_sv_pt']-self.df['l_1st_won']-self.df['l_2nd_won'])/self.df['l_sv_gms']

    def compute_break_points_faced_per_game(self):
        self.new_df['w_bpf_game'],self.new_df['l_bpf_game']=self.df['w_bp_fc']/self.df['w_sv_gms'],self.df['l_bp_fc']/self.df['l_sv_gms']

    def compute_break_points_faced_per_set(self):
        self.new_df['w_bpf_set'],self.new_df['l_bpf_set']=self.df['w_bp_fc']/(self.df['w_sets']+self.df['l_sets']),self.df['l_bp_fc']/(self.df['w_sets']+self.df['l_sets'])

    def compute_service_games_won(self):
        self.new_df['w_svp_game'],self.new_df['l_svp_game']=1-(self.df['w_bp_fc']-self.df['w_bp_sv'])/self.df['w_sv_gms'],1-(self.df['l_bp_fc']-self.df['l_bp_sv'])/self.df['l_sv_gms']
    
    def compute_sv_games_lost_per_set(self):
        self.new_df['w_sv_gm_lset'],self.new_df['l_sv_gm_lset']=(self.df['w_bp_fc']-self.df['w_bp_sv'])/(self.df['w_sets']+self.df['l_sets']),(self.df['l_bp_fc']-self.df['l_bp_sv'])/(self.df['w_sets']+self.df['l_sets'])

    def compute_ip_return_points(self):
        self.new_df['w_r_ip_w'],self.new_df['l_r_ip_w']=1-self.new_df['l_sv_ip_w'],1-self.new_df['w_sv_ip_w']

    def compute_points_won_per_return_game(self):
        self.new_df['w_pwon_r_game'],self.new_df['l_pwon_r_game']=self.new_df['l_pl_game'],self.new_df['w_pl_game']

    def compute_bp_per_game(self):
        self.new_df['w_bp_game'],self.new_df['l_bp_game']=self.new_df['l_bpf_game'],self.new_df['w_bpf_game']

    def compute_bp_per_set(self):
        self.new_df['w_bp_set'],self.new_df['l_bp_set']=self.new_df['l_bpf_set'],self.new_df['w_bpf_set']

    def compute_return_games_won(self):
        self.new_df['w_r_game_w'],self.new_df['l_r_game_w']=1-self.new_df['l_svp_game'],1-self.new_df['w_svp_game']

    def compute_return_games_won_per_set(self):
        self.new_df['w_r_gm_w_set'],self.new_df['l_r_gm_w_set']=self.new_df['l_sv_gm_lset'],self.new_df['w_sv_gm_lset']

    def compute_games_won(self):
        self.new_df['w_gm_w'],self.new_df['l_gm_w']=self.df['w_games']/(self.df['w_sv_gms']+self.df['l_sv_gms']),self.df['l_games']/(self.df['w_sv_gms']+self.df['l_sv_gms'])

    def compute_game_dominance(self):
        self.new_df['w_g_dominance'],self.new_df['l_g_dominance']=self.new_df['w_r_game_w']/self.new_df['l_r_game_w'],self.new_df['l_r_game_w']/self.new_df['w_r_game_w']

    def compute_break_ratio(self):
        self.new_df['w_br_ratio'],self.new_df['l_br_ratio']=self.new_df['w_bp_w']/self.new_df['l_bp_w'],self.new_df['l_bp_w']/self.new_df['w_bp_w']

    def get_dataframe(self):
        return self.new_df

    

    