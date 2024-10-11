import os
import datetime
import random
from pathlib import Path
import polars as pl
import Logger_Config

time = datetime.datetime.now().strftime("%Y.%m.%d - %H.%M.%S")
if not os.path.exists('Logs/'):
    os.makedirs('Logs/')
logger = Logger_Config.configure_logger(f'Logs\\{time} - Payment Data', console_level='DEBUG', file_level='DEBUG', sorting=False)

csv_part_1 = Path.cwd() / '1.csv'
csv_part_2 = Path.cwd() / '2.csv'

showup_fee = 10

DG_1_Keep_More = 10
DG_1_Split = 5
DG_2_Keep_More = 0
DG_2_Split = 5

TG_1_Distrust = 5
TG_1_Punish = 3
TG_1_Reward = 7
TG_2_Distrust = 0
TG_2_Punish = 12
TG_2_Reward = 8

PD_C_CC = 6
PD_D_DC = 7
PD_C_DC = 3
PD_D_DD = 4

piece_rate = 0.5
other_tournament_rate = 1
self_tournament_rate = 1

bonus_rate = 0.5


class Part_df:
    
    def __init__(self, title, input_csv_path):
        self.input_csv_path = input_csv_path
        self.title = title
        self.csv = pl.read_csv(input_csv_path)
    
    def __repr__(self):
        return f'({self.title}:\n{self.df})'
    
    def generate_task_answer_vars(self, base_name, count):  
        for i in range(1, count+1):
            yield f'{base_name}_{i}_1'
            
    def extract_part_df(self, kept_columns):
        
        # New df: Contains only the columns we want to keep
        temp_df = self.csv.select(kept_columns)
        logger.debug(f'\n{self.title} temp_df:\n{temp_df}\n\n')
        
        # List: Combined headers and row[0]
        combined_headers = [f"{self.title} :: {col} :: {temp_df[col][0]}" for col in temp_df.columns]
        
        # New df: Contains the combined headers and the data
        self.df = pl.DataFrame(temp_df[2:], schema=combined_headers)
        logger.debug(f'\n{self.title} df:\n{self.df}\n\n')
        
    def write_df_to_csv(self):
        self.df.write_csv(f'{self.title} - extraction.csv')
        
        
def part_1_main():
    part_1 = Part_df("Part 1", csv_part_1)
    
    kept_columns = [
        "QID127", # G#
        "StartDate",
        "EndDate",
        "Progress",
        "Duration (in seconds)",
        "DistributionChannel",
        "Rand_Scenario_1", # DG Order (A,B,C)
        "Rand_Scenario_2", # TG Order (A,B,C)
        "Rand_Scenario_3", # PD Order (A,B,C)
        "QID1216457213", # Attention check
        "Attention_Check_Correct",
        "Comprehension_Payment",
        "Comprehension_Overview_Correct",
        "Comprehension_Scenario_1_DG_Correct",
        "Comprehension_Scenario_2_TG_Correct",
        "Comprehension_Scenario_3_PD_Correct",
        "Dictator_Game_Choice", # DG choice
        "Trust_Game_P1_Investor_Choice", # TG choice P1 (investor)
        "Trust_Game_P2_Trustee_Choice", # TG choice P2 (trustee)
        "Prisoners_Dilemma_Choice", # PD choice
    ]
    
    part_1.extract_part_df(kept_columns)
    
    return part_1

    
def part_2_main():
    
    part_2 = Part_df("Part 2", csv_part_2)
    
    kept_columns = [
        "QID127", # G#
        "StartDate",
        "EndDate",
        "Progress",
        "Duration (in seconds)",
        "DistributionChannel",
        "QID44_Last Click", # Piece-Rate
        "QID44_Click Count", # Piece-Rate
        "QID51_Last Click", # Other-Tournament
        "QID51_Click Count", # Other-Tournament
        "QID57_Last Click", # Self-Tournament
        "QID57_Click Count", # Self-Tournament
        "QID53_Last Click", # Choice - TG 1
        "QID53_Click Count", # Choice - TG 1
        "QID58_Last Click", # Choice - TG 2
        "QID58_Click Count", # Choice - TG 2
        "QID16", # Student?
        "QID27", # Major?
        "QID17", # Previous ICES experiments?
        "QID23", # Gender?
        "QID29", # Ethnicity?
        "QID24", # Age?
        "QID25_1", # Willingness to take risks?
        "QID25_2", # Willingness to compete with others?
        "QID25_3", # Willingness to compete with yourself?
        "QID26_1", # How socially-oriented?
        "QID26_2", # How generous?
        "QID26_3", # How trustworthy?
        "QID26_4", # How cooperative?
        "QID28_1", # How competitive is your school/workplace?
        "QID30", # Confusion?
        "QID31", # Comments?
        "Showup_Fee",
        "Comprehension_Payment",
        "QID20_1", # How many did you solve in Choice Task?
        "QID38_1", # How many did your Choice Task opponent solve in Other-Tournament Task?
        "QID39_1", # On average, how many did others solve in Piece-Rate Task?
        "QID72_1", # How many did you solve in Other-Tournament Task?
        "QID73_1", # How many problems did you solve in Self-Tournament Task?
        "t1_total_correct",
        "t2_total_correct",
        "t3_total_correct",
        "t4_total_correct", 
        "perc_piece_rate",
        "perc_other_tourn",
        "perc_self_tourn",
        "Treatment_Group",
        "Num_Tasks",
        "Piece_Rate_Num",
        "Other_Tournament_Num",
        "Self_Tournament_Num",
        "Choice_Task_Num",
        "Comprehension_Task_PR_Correct",
        "Comprehension_Task_OT_Correct",
        "Comprehension_Task_ST_Correct",
        "Comprehension_Task_Choice_Group1_Correct",
        "Comprehension_Task_Choice_Group2_Correct",
    ]
    
    task_1_answers = list(part_2.generate_task_answer_vars("QID65", 30)) # Piece-Rate
    task_2_answers = list(part_2.generate_task_answer_vars("QID66", 30)) # Other-Tournament
    task_3_answers = list(part_2.generate_task_answer_vars("QID70", 30)) # Self-Tournament
    task_4_1_answers = list(part_2.generate_task_answer_vars("QID67", 30)) # Choice - TG 1
    task_4_2_answers = list(part_2.generate_task_answer_vars("QID69", 30)) # Choice - TG 2
    
    all_task_answers = task_1_answers + task_2_answers + task_3_answers + task_4_1_answers + task_4_2_answers
    kept_columns.extend(all_task_answers)
    
    part_2.extract_part_df(kept_columns)
    
    return part_2

def calculate_averages(df):
    
    # Stats to calculate:
        # 1. Average number of correct answers for Piece-Rate Task
        # 2. Average number of correct answers for Other-Tournament Task
        # 3. Average number of correct answers for Self-Tournament Task
        # 4. Average number of correct answers for Choice Task
        
    search_str = "total_correct"
    matching_cols = [col for col in df.columns if search_str in col]
    
    averages = ()
    for col in matching_cols:
        
        df = df.with_columns(df[col].replace("", "0"))
        df = df.with_columns(df[col].cast(pl.Int32))        
        
        avg = df[col].mean()
        avg = round(avg, 2)
        df = df.with_columns(pl.lit(avg).alias(f'{col}_average'))
        logger.debug(f'\n{col} average: {avg}\n\n')
    
    return df
    
def calculate_payments(df):
    
    # Steps for calculating payments:
        # 0. Add fixed show-up fee.
        
        # Part 1: Social Game
            # 1. Randomly draw another participant to use for social game.
            # 2. Randomly draw which social game is chosen for payment.
            # 3. Apply partner's choices to chosen social game.
            # 4. Add payment for chosen social game.
            
        # Part 2: Summation Task
            # 1. Tasks:
                # a. Piece-Rate:
                    # i. Calculate payment based on number of correct answers.
                # b. Other-Tournament:
                        # i. Randomly draw opponent.
                        # ii. Determine if score is higher than opponent's.
                        # iii. Calculate payment based on number of correct answers × 1 if score is higher, else 0.
                # c. Self-Tournament:
                        # i. Determine if score is higher than previous score.
                        # ii. Calculate payment based on number of correct answers × 1 if score is higher, else 0.
                # d. Choice Task:
                    # i. Randomly draw new opponent.
                    # ii. Determine if score is higher than opponent's.
                    # iii. Determine if score is higher than previous score.
                    # iv. Calculate payment proportional to chosen payment combination.
            # 2. Randomly draw which task is chosen for payment.
            # 3. Add payment for chosen task.
            
        # Bonus Payments
            # 1. Add payment × comprehension question correct.
            # 2. Add payment × correct guesses about performance.
            
    g_nums = df[[col for col in df.columns if 'G#' in col][0]].to_list()
    
    # Part 1: Social Game
    
    social_game_partners = [random.choice([g for g in g_nums if g != current_g]) for current_g in g_nums]
    logger.debug(f'\nSocial Game Partners:\n{social_game_partners}\n\n')
    
    for a, b in zip(g_nums, social_game_partners):
        if a == b:
            logger.debug(f'\n{a} vs {b}\n\n')
            raise ValueError(f'{a} == {b}: Social game partner is self.')
    
    random_social_game = [random.choice(['DG_1', 'DG_2', 'TG_1', 'TG_2', 'PD_1', 'PD_2']) for _ in g_nums]
    logger.debug(f'\nRandom Social Games:\n{random_social_game}\n\n')
    
    payments = df[[col for col in df.columns if 'Showup_Fee' in col][0]].to_list()
    payments = [p[1:] if p[0] == '$' else p for p in payments]
    payments = [round(float(p), 2) for p in payments]
    
    game_payments = []
    game_self_choices = []
    game_partners_choices = []
    for idx, (game, partner) in enumerate(zip(random_social_game, social_game_partners)):
        partner_index = g_nums.index(partner)
        
        if game == 'DG_1':
            self_choice = df[[col for col in df.columns if 'Dictator_Game_Choice' in col][0]][idx]
            partner_choice = df[[col for col in df.columns if 'Dictator_Game_Choice' in col][0]][partner_index]
            if self_choice == 'Keep more':
                game_payment = DG_1_Keep_More
            elif self_choice == 'Split':
                game_payment = DG_1_Split
        
        elif game == 'DG_2':
            self_choice = df[[col for col in df.columns if 'Dictator_Game_Choice' in col][0]][idx]
            partner_choice = df[[col for col in df.columns if 'Dictator_Game_Choice' in col][0]][partner_index]
            if partner_choice == 'Keep more':
                game_payment = DG_2_Keep_More
            elif partner_choice == 'Split':
                game_payment = DG_2_Split
        
        elif game == 'TG_1':
            self_choice = df[[col for col in df.columns if 'Trust_Game_P1_Investor_Choice' in col][0]][idx]
            partner_choice = df[[col for col in df.columns if 'Trust_Game_P2_Trustee_Choice' in col][0]][partner_index]
            if self_choice == 'Distrust':
                game_payment = TG_1_Distrust
            elif self_choice == 'Trust':
                if partner_choice == 'Punish':
                    game_payment = TG_1_Punish
                elif partner_choice == 'Reward':
                    game_payment = TG_1_Reward
        
        elif game == 'TG_2':
            self_choice = df[[col for col in df.columns if 'Trust_Game_P2_Trustee_Choice' in col][0]][idx]
            partner_choice = df[[col for col in df.columns if 'Trust_Game_P1_Investor_Choice' in col][0]][partner_index]
            if partner_choice == 'Distrust':
                game_payment = TG_2_Distrust
            elif partner_choice == 'Trust':
                if self_choice == 'Punish':
                    game_payment = TG_2_Punish
                elif self_choice == 'Reward':
                    game_payment = TG_2_Reward

        elif game == 'PD_1' or game == 'PD_2':
            self_choice = df[[col for col in df.columns if 'Prisoners_Dilemma_Choice' in col][0]][idx]
            partner_choice = df[[col for col in df.columns if 'Prisoners_Dilemma_Choice' in col][0]][partner_index]
            if self_choice == 'Cooperate':
                if partner_choice == 'Cooperate':
                    game_payment = PD_C_CC
                elif partner_choice == 'Defect':
                    game_payment = PD_C_DC
            elif self_choice == 'Defect':
                if partner_choice == 'Cooperate':
                    game_payment = PD_D_DC
                elif partner_choice == 'Defect':
                    game_payment = PD_D_DD
        
        game_self_choices.append(self_choice)
        game_partners_choices.append(partner_choice)
        game_payments.append(game_payment)
        payments[idx] += game_payment
        
        logger.info(f'Subject {idx}: Game: {game}, Partner: {partner}, Partners Choice: {partner_choice}, Self Choice: {self_choice}, Payment: {game_payment}')

    # Part 2: Summation Task
    
    # a. Piece-Rate
    piece_rate_correct = df[[col for col in df.columns if 't1_total_correct' in col][0]].to_list()
    piece_rate_correct = [int(p) for p in piece_rate_correct]
    piece_rate_payments = [p * piece_rate for p in piece_rate_correct]
    piece_rate_payments = [float(round(p, 2)) for p in piece_rate_payments]
    
    # b. Other-Tournament
    other_tournament_correct = df[[col for col in df.columns if 't2_total_correct' in col][0]].to_list()
    other_tournament_correct = [int(p) for p in other_tournament_correct]
    
    task_2_opponents = [random.choice([g for g in g_nums if g != current_g]) for current_g in g_nums]
    logger.debug(f'\nTask 2 Opponents:\n{task_2_opponents}\n\n')
    
    for a, b in zip(g_nums, task_2_opponents):
        if a == b:
            logger.debug(f'\n{a} vs {b}\n\n')
            raise ValueError(f'{a} == {b}: Task 2 opponent is self.')
        
    other_tournament_payments = []
    for idx, (correct, opponent) in enumerate(zip(other_tournament_correct, task_2_opponents)):
        opponent_index = g_nums.index(opponent)
        opponent_correct = int(other_tournament_correct[opponent_index])        
        if correct > opponent_correct:
            payment = correct * other_tournament_rate
        elif correct == opponent_correct:
            payment = correct * other_tournament_rate * 0.5
        elif correct < opponent_correct:
            payment = correct * other_tournament_rate * 0
        
        payment = float(round(payment, 2))
        other_tournament_payments.append(payment)
        logger.debug(f'Subject {idx}: Correct: {correct}, Opponent: {opponent}, Opponent Correct: {opponent_correct}, Payment: {payment}')
        
    # c. Self-Tournament
    self_tournament_correct = df[[col for col in df.columns if 't3_total_correct' in col][0]].to_list()
    self_tournament_correct = [0 if p == None or p == -1 else int(p) for p in self_tournament_correct]
        
    self_tournament_payments = []
    for idx, (correct, previous) in enumerate(zip(self_tournament_correct, piece_rate_correct)):
        if correct > previous:
            payment = correct * self_tournament_rate
        elif correct == previous:
            payment = correct * self_tournament_rate * 0.5
        elif correct < previous:
            payment = correct * self_tournament_rate * 0
        
        payment = float(round(payment, 2))
        self_tournament_payments.append(payment)
        logger.debug(f'Subject {idx}: Self-Tournament Correct: {correct}, Previous: {previous}, Payment: {payment}')
        
    # d. Choice Task
    task_4_correct = df[[col for col in df.columns if 't4_total_correct' in col][0]].to_list()
    task_4_correct = [int(p) for p in task_4_correct]
            
    task_4_opponents = [random.choice([g for g in g_nums if g != current_g]) for current_g in g_nums]
    logger.debug(f'\nTask 4 Opponents:\n{task_4_opponents}\n\n')
    
    for a, b in zip(g_nums, task_4_opponents):
        if a == b:
            logger.debug(f'\n{a} vs {b}\n\n')
            raise ValueError(f'{a} == {b}: Task 4 opponent is self.')
    
    perc_piece_rate = df[[col for col in df.columns if 'perc_piece_rate' in col][0]].to_list()
    perc_piece_rate = [int(p) for p in perc_piece_rate]
    perc_other_tourn = df[[col for col in df.columns if 'perc_other_tourn' in col][0]].to_list()
    perc_other_tourn = [int(p) for p in perc_other_tourn]
    perc_self_tourn = df[[col for col in df.columns if 'perc_self_tourn' in col][0]].to_list()
    perc_self_tourn = [0 if p == None or p == -1 else p for p in perc_self_tourn]
    perc_self_tourn = [int(p) for p in perc_self_tourn] 
    
    task_4_potential_piece_rate_payments = [p * piece_rate for p in task_4_correct]
    task_4_potential_piece_rate_payments = [float(round(p, 2)) for p in task_4_potential_piece_rate_payments]
    task_4_potential_other_tournament_payments = []
    task_4_potential_self_tournament_payments = []
    task_4_payments = []
    
    for idx, (correct, task_4_opponent) in enumerate(zip(task_4_correct, task_4_opponents)):
                
        # Potential Other-Tournament Payment: Comparison with opponent's Task 2 performance.
        opponent_correct = int(other_tournament_correct[g_nums.index(task_4_opponent)])
        if correct > opponent_correct:
            potential_other_tournament_payment = correct * other_tournament_rate
        elif correct == opponent_correct:
            potential_other_tournament_payment = correct * other_tournament_rate * 0.5
        elif correct < opponent_correct:
            potential_other_tournament_payment = correct * other_tournament_rate * 0
        
        potential_other_tournament_payment = float(round(potential_other_tournament_payment, 2))
        task_4_potential_other_tournament_payments.append(potential_other_tournament_payment)
        
        # Potential Self-Tournament Payment: Comparison with self Task 2 performance.
        previous_correct = other_tournament_correct[idx]
        if correct > previous_correct:
            potential_self_tournament_payment = correct * self_tournament_rate
        elif correct == previous_correct:
            potential_self_tournament_payment = correct * self_tournament_rate * 0.5
        elif correct < previous_correct:
            potential_self_tournament_payment = correct * self_tournament_rate * 0
        
        potential_self_tournament_payment = float(round(potential_self_tournament_payment, 2))
        task_4_potential_self_tournament_payments.append(potential_self_tournament_payment)
        
        # Calculating actual payments based on chosen payment combination.
        piece_rate_payment = perc_piece_rate[idx] * task_4_potential_piece_rate_payments[idx] / 100
        other_tournament_payment = perc_other_tourn[idx] * potential_other_tournament_payment / 100
        self_tournament_payment = perc_self_tourn[idx] * potential_self_tournament_payment / 100
        
        payment = piece_rate_payment + other_tournament_payment + self_tournament_payment
        payment = float(round(payment, 2))
        task_4_payments.append(payment)
        
        logger.debug(f'CHOICE TASK: Subject {idx}:\nCorrect: {correct}, Opponent Correct: {opponent_correct}, Previous Correct: {previous_correct}, Opponent: {task_4_opponent};\n'
                     f'Potential Piece-Rate: {task_4_potential_piece_rate_payments[idx]}, Potential Other-Tourn: {potential_other_tournament_payment}, Potential Self-Tourn: {potential_self_tournament_payment};\n'
                     f'Piece-Rate Percent: {perc_piece_rate[idx]/100}, Other-Tourn Percent: {perc_other_tourn[idx]/100}, Self-Tourn Percent: {perc_self_tourn[idx]/100};\n'
                     f'Piece-Rate Payment: {piece_rate_payment}, Other-Tourn Payment: {other_tournament_payment}, Self-Tourn Payment: {self_tournament_payment};\n'
                     f'Final Choice Task Payment: {payment}\n')
    
    treatment_groups = df[[col for col in df.columns if 'Treatment_Group' in col][0]].to_list()
    rand_tasks = []
    task_payments = []
    for idx in range(len(g_nums)):
        if treatment_groups[idx] == '1':
            rand_task_for_payment = random.choice(['Piece-Rate', 'Other-Tournament', 'Choice-Task'])
        elif treatment_groups[idx] == '2':
            rand_task_for_payment = random.choice(['Piece-Rate', 'Other-Tournament', 'Self-Tournament', 'Choice-Task'])
        
        if rand_task_for_payment == 'Piece-Rate':
            task_payment = piece_rate_payments[idx]
        elif rand_task_for_payment == 'Other-Tournament':
            task_payment = other_tournament_payments[idx]
        elif rand_task_for_payment == 'Self-Tournament':
            task_payment = self_tournament_payments[idx]
        elif rand_task_for_payment == 'Choice-Task':
            task_payment = task_4_payments[idx]
        
        rand_tasks.append(rand_task_for_payment)
        payments[idx] += task_payment
        task_payments.append(task_payment)
        logger.info(f'Subject {idx}: Random Task for Payment: {rand_task_for_payment}; Task Payment: {task_payment}')
    
    # Bonus Payments: Comprehension Questions and Performance Guesses
    comprehension_questions = [
        'Comprehension_Overview_Correct',
        'Comprehension_Scenario_1_DG_Correct',
        'Comprehension_Scenario_2_TG_Correct',
        'Comprehension_Scenario_3_PD_Correct',
        'Comprehension_Task_PR_Correct',
        'Comprehension_Task_OT_Correct',
        'Comprehension_Task_ST_Correct',
        'Comprehension_Task_Choice_Group1_Correct',
        'Comprehension_Task_Choice_Group2_Correct',
    ]
    
    bonus_payments = []
    for idx in range(len(g_nums)):
        bonus = 0
        for question in comprehension_questions:
            if df[[col for col in df.columns if question in col][0]][idx] in (1, '1'):
                bonus += bonus_rate
                
        logger.info(f'Subject {idx}: Comprehension Questions Correct: {int(bonus/bonus_rate)}. Bonus: $ {bonus:.2f}.')
                
        task_4_guess_self = df[[col for col in df.columns if 'QID20_1' in col][0]][idx]
        task_4_guess_self = int(task_4_guess_self)
        if task_4_guess_self - 1 <= task_4_correct[idx] <= task_4_guess_self + 1:
            bonus += bonus_rate
            logger.info(f'Subject {idx}: Task 4 Guess (Correct): {task_4_guess_self} ({task_4_correct[idx]}). Bonus: $ {bonus_rate:.2f}.')
        else:
            logger.debug(f'Subject {idx}: Task 4 Guess (Incorrect): {task_4_guess_self} ({task_4_correct[idx]}). No Bonus.')
        
        task_4_guess_opponent = df[[col for col in df.columns if 'QID38_1' in col][0]][idx]
        task_4_guess_opponent = int(task_4_guess_opponent)
        task_4_opponent_correct = int(other_tournament_correct[g_nums.index(task_4_opponents[idx])])
        if task_4_guess_opponent - 1 <= task_4_opponent_correct <= task_4_guess_opponent + 1:
            bonus += bonus_rate
            logger.info(f'Subject {idx}: Task 4 Opponent Guess (Correct): {task_4_guess_opponent} ({task_4_opponent_correct}). Bonus: $ {bonus_rate:.2f}.')
        else:
            logger.debug(f'Subject {idx}: Task 4 Opponent Guess (Incorrect): {task_4_guess_opponent} ({task_4_opponent_correct}). No Bonus.')
        
        task_1_group_average = df[[col for col in df.columns if 't1_total_correct' in col][0]].mean()
        task_1_group_average = round(task_1_group_average, 2)
        task_1_guess_average = df[[col for col in df.columns if 'QID39_1' in col][0]][idx]
        task_1_guess_average = int(task_1_guess_average)
        if task_1_guess_average - 1 <= task_1_group_average <= task_1_guess_average + 1:
            bonus += bonus_rate
            logger.info(f'Subject {idx}: Task 1 Guess Average (Correct): {task_1_guess_average} ({task_1_group_average}). Bonus: $ {bonus_rate:.2f}.')
        else:
            logger.debug(f'Subject {idx}: Task 1 Guess Average (Incorrect): {task_1_guess_average} ({task_1_group_average}). No Bonus.')
        
        task_2_guess_self = df[[col for col in df.columns if 'QID72_1' in col][0]][idx]
        task_2_guess_self = int(task_2_guess_self)
        if task_2_guess_self - 1 <= other_tournament_correct[idx] <= task_2_guess_self + 1:
            bonus += bonus_rate
            logger.info(f'Subject {idx}: Task 2 Guess (Correct): {task_2_guess_self} ({other_tournament_correct[idx]}). Bonus: $ {bonus_rate:.2f}.')
        else:
            logger.debug(f'Subject {idx}: Task 2 Guess (Incorrect): {task_2_guess_self} ({other_tournament_correct[idx]}). No Bonus.')
            
        task_3_guess_self = df[[col for col in df.columns if 'QID73_1' in col][0]][idx]
        if task_3_guess_self is not None:
            task_3_guess_self = int(task_3_guess_self)
            if task_3_guess_self - 1 <= self_tournament_correct[idx] <= task_3_guess_self + 1:
                bonus += bonus_rate
                logger.info(f'Subject {idx}: Task 3 Guess (Correct): {task_3_guess_self} ({self_tournament_correct[idx]}). Bonus: $ {bonus_rate:.2f}.')
            else:
                logger.debug(f'Subject {idx}: Task 3 Guess (Incorrect): {task_3_guess_self} ({self_tournament_correct[idx]}). No Bonus.')
                
        payments[idx] += bonus
        payments = [float(round(p, 2)) for p in payments]
        bonus_payments.append(bonus)
        logger.info(f'Subject {idx}: Total Bonus: $ {bonus:.2f}.')
        
    for idx, payment in enumerate(payments):
        logger.info(f'Subject {idx}: Showup Fee: $ {showup_fee}. Game Payment: $ {game_payments[idx]}. Task Payment: $ {task_payments[idx]}. Bonus Payment: $ {bonus_payments[idx]}.\n'
                    f'Total Payment: $ {payment:.2f}.\n'
                    f'Math check: $ {showup_fee + game_payments[idx] + task_payments[idx] + bonus_payments[idx]:.2f}.')
    
    to_append = [social_game_partners, random_social_game, game_self_choices, game_partners_choices,
                 piece_rate_payments, task_2_opponents, other_tournament_payments, self_tournament_payments, task_4_opponents, task_4_payments,
                 task_4_potential_piece_rate_payments, task_4_potential_other_tournament_payments, task_4_potential_self_tournament_payments, task_4_payments,
                 rand_tasks, task_payments, game_payments, bonus_payments, payments]    
    
    to_append_series = [
        pl.Series("social_game_partners", social_game_partners),
        pl.Series("random_social_game", random_social_game),
        pl.Series("game_self_choices", game_self_choices),
        pl.Series("game_partners_choices", game_partners_choices),
        pl.Series("piece_rate_payments", piece_rate_payments),
        pl.Series("task_2_opponents", task_2_opponents),
        pl.Series("other_tournament_payments", other_tournament_payments),
        pl.Series("self_tournament_payments", self_tournament_payments),
        pl.Series("task_4_opponents", task_4_opponents),
        pl.Series("task_4_payments", task_4_payments),
        pl.Series("task_4_potential_piece_rate_payments", task_4_potential_piece_rate_payments),
        pl.Series("task_4_potential_other_tournament_payments", task_4_potential_other_tournament_payments),
        pl.Series("task_4_potential_self_tournament_payments", task_4_potential_self_tournament_payments),
        pl.Series("rand_tasks", rand_tasks),
        pl.Series("task_payments", task_payments),
        pl.Series("game_payments", game_payments),
        pl.Series("bonus_payments", bonus_payments),
        pl.Series("payments", payments),
    ]
    
    df = df.with_columns(to_append_series)
    
    return df
    
def extract_payments(df):

    kept_columns = [ #TODO: It might be cleaner to use a dict with aliases.
        'Part 1 :: G#',
        'Treatment_Group',
        'Comprehension_Overview_Correct',
        'Comprehension_Scenario_1_DG_Correct',
        'Comprehension_Scenario_2_TG_Correct',
        'Comprehension_Scenario_3_PD_Correct',
        'Comprehension_Task_PR_Correct',
        'Comprehension_Task_OT_Correct',
        'Comprehension_Task_ST_Correct',
        'Comprehension_Task_Choice_Group1_Correct',
        'Comprehension_Task_Choice_Group2_Correct',
        
        'QID72_1', # Guess Task 2 of Self
        'QID73_1', # Guess Task 3 of Self
        'QID38_1', # Guess Task 2 of Task 4 Opponent
        'QID39_1', # Guess Average Task 1 of Group
        'QID20_1', # Guess Task 4 of Self
        't1_total_correct_average',
        't2_total_correct_average',
        't3_total_correct_average',
        't4_total_correct_average',

        't1_total_correct',
        'piece_rate_payments',
        't2_total_correct',
        'other_tournament_payments',
        't3_total_correct',
        'self_tournament_payments',
        't4_total_correct',
        'task_4_potential_piece_rate_payments',
        'perc_piece_rate',
        'task_4_potential_other_tournament_payments',
        'perc_other_tourn',
        'task_4_potential_self_tournament_payments',
        'perc_self_tourn',
        'task_4_payments',

        'Part 2 :: Showup_Fee',
        'random_social_game',
        'game_self_choices',
        'game_partner_choices',
        'game_payments',
        'rand_tasks',
        'task_payments',
        'bonus_payments',
        'payments',

        'Part 2 :: G#'
    ]

    # Select columns based on partial matches and order them as specified in kept_columns
    selected_columns = []
    for partial in kept_columns:
        matched_columns = [col for col in df.columns if partial in col]
        selected_columns.extend(matched_columns)

    # Remove duplicate columns
    selected_columns = list(dict.fromkeys(selected_columns))

    payment_df = df.select([pl.col(col) for col in selected_columns])

    return payment_df


if __name__ == "__main__":
    
    part_1 = part_1_main()
    part_2 = part_2_main()
    
    part_1.df = part_1.df.rename({part_1.df.columns[0]: "Part 1 :: G#"})
    part_2.df = part_2.df.rename({part_2.df.columns[0]: "Part 2 :: G#"})
    part_2.df = part_2.df.with_columns(pl.col("Part 2 :: G#").alias("G#"))
    
    df = part_1.df.join(part_2.df, left_on="Part 1 :: G#", right_on="G#", how="inner")
    
    df = calculate_averages(df)
    df = calculate_payments(df)
    payment_df = extract_payments(df)
    df.write_csv(f'{time} - Full Data (Processed).csv')
    payment_df.write_csv(f'{time} - Payments.csv')